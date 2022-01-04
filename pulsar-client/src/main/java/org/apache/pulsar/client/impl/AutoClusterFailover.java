/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.base.Strings;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AutoClusterFailoverBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
@Data
public class AutoClusterFailover implements ServiceUrlProvider {
    private PulsarClient pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private final String primary;
    private final String secondary;
    private final Authentication primaryAuthentication;
    private final Authentication secondaryAuthentication;
    private final String primaryTlsTrustCertsFilePath;
    private final String secondaryTlsTrustCertsFilePath;
    private String primaryTlsTrustStorePath;
    private String secondaryTlsTrustStorePath;
    private String primaryTlsTrustStorePassword;
    private String secondaryTlsTrustStorePassword;
    private final long failoverDelayNs;
    private final long switchBackDelayNs;
    private final ScheduledExecutorService executor;
    private long recoverTimestamp;
    private long failedTimestamp;
    private final int interval = 30_000;
    private static final int TIMEOUT = 30_000;

    private AutoClusterFailover(String primary, String secondary, long failoverDelayNs, long switchBackDelayNs,
                                Authentication primaryAuthentication, Authentication secondaryAuthentication,
                                String primaryTlsTrustCertsFilePath, String secondaryTlsTrustCertsFilePath,
                                String primaryTlsTrustStorePath, String secondaryTlsTrustStorePath,
                                String primaryTlsTrustStorePassword, String secondaryTlsTrustStorePassword) {
        this.primary = primary;
        this.secondary = secondary;
        this.primaryAuthentication = primaryAuthentication;
        this.secondaryAuthentication = secondaryAuthentication;
        this.primaryTlsTrustCertsFilePath = primaryTlsTrustCertsFilePath;
        this.secondaryTlsTrustCertsFilePath = secondaryTlsTrustCertsFilePath;
        this.primaryTlsTrustStorePath = primaryTlsTrustStorePath;
        this.secondaryTlsTrustStorePath = secondaryTlsTrustStorePath;
        this.primaryTlsTrustStorePassword = primaryTlsTrustStorePassword;
        this.secondaryTlsTrustStorePassword = secondaryTlsTrustStorePassword;
        this.failoverDelayNs = failoverDelayNs;
        this.switchBackDelayNs = switchBackDelayNs;
        this.currentPulsarServiceUrl = primary;
        this.recoverTimestamp = -1;
        this.failedTimestamp = -1;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-service-provider"));
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = client;

        // start to probe primary cluster active or not
        this.executor.scheduleAtFixedRate(catchingAndLoggingThrowables(() -> {
            if (currentPulsarServiceUrl.equals(primary)) {
                // current service url is primary, probe whether it is down
                probeAndUpdateServiceUrl(secondary, secondaryAuthentication, secondaryTlsTrustCertsFilePath,
                        secondaryTlsTrustStorePath, secondaryTlsTrustStorePassword);
            } else {
                // current service url is secondary, probe whether it is down
                probeAndUpdateServiceUrl(primary, primaryAuthentication, primaryTlsTrustCertsFilePath,
                        primaryTlsTrustStorePath, primaryTlsTrustStorePassword);
                // secondary cluster is up, check whether need to switch back to primary
                probeAndCheckSwitchBack(primary, primaryAuthentication, primaryTlsTrustCertsFilePath,
                        primaryTlsTrustStorePath, primaryTlsTrustStorePassword);
            }
        }), getInterval(), getInterval(), TimeUnit.MILLISECONDS);

    }

    @Override
    public String getServiceUrl() {
        return this.currentPulsarServiceUrl;
    }

    @Override
    public void close() {
        this.executor.shutdown();
    }

    boolean probeAvailable(String url) {
        try {
            String hostAndPort = parseHostAndPort(url);
            if (Strings.isNullOrEmpty(hostAndPort)) {
                return false;
            }

            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(parseHost(hostAndPort), parsePort(hostAndPort)), TIMEOUT);
            socket.close();
            return true;
        } catch (Exception e) {
            log.warn("Failed to probe available, url: {}", url, e);
            return false;
        }
    }

    private static String parseHostAndPort(String url) {
        if (Strings.isNullOrEmpty(url) || !url.startsWith("pulsar")) {
            throw new IllegalArgumentException("'" + url + "' isn't an Pulsar service URL");
        }

        int uriSeparatorPos = url.indexOf("://");
        if (uriSeparatorPos == -1) {
            throw new IllegalArgumentException("'" + url + "' isn't an URI.");
        }
        return url.substring(uriSeparatorPos + 3);
    }

    private static String parseHost(String hostAndPort) {
        int portSeparatorPos = hostAndPort.indexOf(":");
        if (portSeparatorPos == -1) {
            throw new IllegalArgumentException("'" + hostAndPort + "' isn't an URI.");
        }
        return hostAndPort.substring(0, portSeparatorPos);
    }

    private static Integer parsePort(String hostAndPort) {
        int portSeparatorPos = hostAndPort.indexOf(":");
        if (portSeparatorPos == -1) {
            throw new IllegalArgumentException("'" + hostAndPort + "' isn't an URI.");
        }
        return Integer.valueOf(hostAndPort.substring(portSeparatorPos+1));
    }

    private static long nanosToMillis(long nanos) {
        return Math.max(0L, Math.round(nanos / 1_000_000.0d));
    }

    private void updateServiceUrl(String target,
                                  Authentication authentication,
                                  String tlsTrustCertsFilePath,
                                  String tlsTrustStorePath,
                                  String tlsTrustStorePassword) {
        try {
            if (!Strings.isNullOrEmpty(tlsTrustCertsFilePath)) {
                pulsarClient.updateTlsTrustCertsFilePath(tlsTrustCertsFilePath);
            }

            if (authentication != null) {
                pulsarClient.updateAuthentication(authentication);
            }

            if (!Strings.isNullOrEmpty(tlsTrustStorePath)) {
                pulsarClient.updateTlsTrustStorePathAndPassword(tlsTrustStorePath, tlsTrustStorePassword);
            }

            pulsarClient.updateServiceUrl(target);
            currentPulsarServiceUrl = target;
        } catch (IOException e) {
            log.error("Current Pulsar service is {}, "
                    + "failed to switch back to {} ", currentPulsarServiceUrl, target, e);
        }
    }

    private void probeAndUpdateServiceUrl(String targetServiceUrl,
                                          Authentication authentication,
                                          String tlsTrustCertsFilePath,
                                          String tlsTrustStorePath,
                                          String tlsTrustStorePassword) {
        if (probeAvailable(currentPulsarServiceUrl)) {
            failedTimestamp = -1;
            return;
        }

        long currentTimestamp = System.nanoTime();
        if (failedTimestamp == -1) {
            failedTimestamp = currentTimestamp;
        } else if (currentTimestamp - failedTimestamp >= failoverDelayNs) {
            if (probeAvailable(targetServiceUrl)) {
                log.info("Current Pulsar service is {}, it has been down for {} ms, "
                                + "switch to the service {}. The current service down at {}",
                        currentPulsarServiceUrl, nanosToMillis(currentTimestamp - failedTimestamp),
                        targetServiceUrl, failedTimestamp);
                updateServiceUrl(targetServiceUrl, authentication, tlsTrustCertsFilePath,
                        tlsTrustStorePath, tlsTrustStorePassword);
                failedTimestamp = -1;
            } else {
                log.error("Current Pulsar service is {}, it has been down for {} ms. "
                                + "Failed to switch to service {}, "
                                + "because it is not available",
                        currentPulsarServiceUrl, nanosToMillis(currentTimestamp - failedTimestamp),
                        targetServiceUrl);
            }
        }
    }

    private void probeAndCheckSwitchBack(String target,
                                         Authentication authentication,
                                         String tlsTrustCertsFilePath,
                                         String tlsTrustStorePath,
                                         String tlsTrustStorePassword) {
        long currentTimestamp = System.nanoTime();
        if (!probeAvailable(target)) {
            recoverTimestamp = -1;
            return;
        }

        if (recoverTimestamp == -1) {
            recoverTimestamp = currentTimestamp;
        } else if (currentTimestamp - recoverTimestamp >= switchBackDelayNs) {
            log.info("Current Pulsar service is secondary: {}, "
                            + "the primary service: {} has been recover for {} ms, "
                            + "switch back to the primary service",
                    currentPulsarServiceUrl, target, nanosToMillis(currentTimestamp - recoverTimestamp));
            updateServiceUrl(target, authentication, tlsTrustCertsFilePath, tlsTrustStorePath, tlsTrustStorePassword);
            recoverTimestamp = -1;
        }
    }

    public static class AutoClusterFailoverBuilderImpl implements AutoClusterFailoverBuilder {
        private String primary;
        private String secondary;
        private Authentication primaryAuthentication = null;
        private Authentication secondaryAuthentication = null;
        private String primaryTlsTrustCertsFilePath = null;
        private String secondaryTlsTrustCertsFilePath = null;
        private String primaryTlsTrustStorePath = null;
        private String secondaryTlsTrustStorePath = null;
        private String primaryTlsTrustStorePassword = null;
        private String secondaryTlsTrustStorePassword = null;
        private long failoverDelayNs;
        private long switchBackDelayNs;


        public AutoClusterFailoverBuilder primary(String primary) {
            this.primary = primary;
            return this;
        }

        public AutoClusterFailoverBuilder secondary(String secondary) {
            this.secondary = secondary;
            return this;
        }

        public AutoClusterFailoverBuilder primaryAuthentication(Authentication authentication) {
            this.primaryAuthentication = authentication;
            return this;
        }

        public AutoClusterFailoverBuilder secondaryAuthentication(Authentication authentication) {
            this.secondaryAuthentication = authentication;
            return this;
        }

        public AutoClusterFailoverBuilder primaryTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
            this.primaryTlsTrustCertsFilePath = tlsTrustCertsFilePath;
            return this;
        }

        public AutoClusterFailoverBuilder secondaryTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
            this.secondaryTlsTrustCertsFilePath = tlsTrustCertsFilePath;
            return this;
        }

        public AutoClusterFailoverBuilder primaryTlsTrustStorePath(String tlsTrustStorePath) {
            this.primaryTlsTrustStorePath = tlsTrustStorePath;
            return this;
        }

        public AutoClusterFailoverBuilder secondaryTlsTrustStorePath(String tlsTrustStorePath) {
            this.secondaryTlsTrustStorePath = tlsTrustStorePath;
            return this;
        }

        public AutoClusterFailoverBuilder primaryTlsTrustStorePassword(String tlsTrustStorePassword) {
            this.primaryTlsTrustStorePassword = tlsTrustStorePassword;
            return this;
        }

        public AutoClusterFailoverBuilder secondaryTlsTrustStorePassword(String tlsTrustStorePassword) {
            this.secondaryTlsTrustStorePassword = tlsTrustStorePassword;
            return this;
        }

        public AutoClusterFailoverBuilder failoverDelay(long failoverDelay, TimeUnit timeUnit) {
            this.failoverDelayNs = timeUnit.toNanos(failoverDelay);
            return this;
        }

        public AutoClusterFailoverBuilder switchBackDelay(long switchBackDelay, TimeUnit timeUnit) {
            this.switchBackDelayNs = timeUnit.toNanos(switchBackDelay);
            return this;
        }

        public ServiceUrlProvider build() {
            Objects.requireNonNull(primary, "primary service url shouldn't be null");
            Objects.requireNonNull(secondary, "secondary service url shouldn't be null");
            checkArgument(failoverDelayNs >= 0, "failoverDelayMs should >= 0");
            checkArgument(switchBackDelayNs >= 0, "switchBackDelayMs should >= 0");

            return new AutoClusterFailover(primary, secondary, failoverDelayNs, switchBackDelayNs,
                    primaryAuthentication, secondaryAuthentication,
                    primaryTlsTrustCertsFilePath, secondaryTlsTrustCertsFilePath,
                    primaryTlsTrustStorePath, secondaryTlsTrustStorePath,
                    primaryTlsTrustStorePassword, secondaryTlsTrustStorePassword);
        }

        public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
            if (!expression) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
        }
    }

    public static AutoClusterFailoverBuilder builder() {
        return new AutoClusterFailoverBuilderImpl();
    }
}
