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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.NonNull;
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
    private final List<String> secondary;
    private final Authentication primaryAuthentication;
    private final List<Authentication> secondaryAuthentications;
    private final String primaryTlsTrustCertsFilePath;
    private final List<String> secondaryTlsTrustCertsFilePaths;
    private String primaryTlsTrustStorePath;
    private List<String> secondaryTlsTrustStorePaths;
    private String primaryTlsTrustStorePassword;
    private List<String> secondaryTlsTrustStorePasswords;
    private final long failoverDelayNs;
    private final long switchBackDelayNs;
    private final ScheduledExecutorService executor;
    private long recoverTimestamp;
    private long failedTimestamp;
    private final long intervalMs;
    private static final int TIMEOUT = 30_000;

    private AutoClusterFailover(String primary, List<String> secondary, long failoverDelayNs, long switchBackDelayNs,
                                long intervalMs, Authentication primaryAuthentication,
                                List<Authentication> secondaryAuthentications, String primaryTlsTrustCertsFilePath,
                                List<String> secondaryTlsTrustCertsFilePaths, String primaryTlsTrustStorePath,
                                List<String> secondaryTlsTrustStorePaths, String primaryTlsTrustStorePassword,
                                List<String> secondaryTlsTrustStorePasswords) {
        this.primary = primary;
        this.secondary = secondary;
        this.primaryAuthentication = primaryAuthentication;
        this.secondaryAuthentications = secondaryAuthentications;
        this.primaryTlsTrustCertsFilePath = primaryTlsTrustCertsFilePath;
        this.secondaryTlsTrustCertsFilePaths = secondaryTlsTrustCertsFilePaths;
        this.primaryTlsTrustStorePath = primaryTlsTrustStorePath;
        this.secondaryTlsTrustStorePaths = secondaryTlsTrustStorePaths;
        this.primaryTlsTrustStorePassword = primaryTlsTrustStorePassword;
        this.secondaryTlsTrustStorePasswords = secondaryTlsTrustStorePasswords;
        this.failoverDelayNs = failoverDelayNs;
        this.switchBackDelayNs = switchBackDelayNs;
        this.currentPulsarServiceUrl = primary;
        this.recoverTimestamp = -1;
        this.failedTimestamp = -1;
        this.intervalMs = intervalMs;
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
                probeAndUpdateServiceUrl(secondary, secondaryAuthentications, secondaryTlsTrustCertsFilePaths,
                        secondaryTlsTrustStorePaths, secondaryTlsTrustStorePasswords);
            } else {
                // current service url is secondary, probe whether it is down
                probeAndUpdateServiceUrl(primary, primaryAuthentication, primaryTlsTrustCertsFilePath,
                        primaryTlsTrustStorePath, primaryTlsTrustStorePassword);
                // secondary cluster is up, check whether need to switch back to primary
                probeAndCheckSwitchBack(primary, primaryAuthentication, primaryTlsTrustCertsFilePath,
                        primaryTlsTrustStorePath, primaryTlsTrustStorePassword);
            }
        }), intervalMs, intervalMs, TimeUnit.MILLISECONDS);

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

    private void probeAndUpdateServiceUrl(List<String> targetServiceUrls,
                                          List<Authentication> authentications,
                                          List<String> tlsTrustCertsFilePaths,
                                          List<String> tlsTrustStorePaths,
                                          List<String> tlsTrustStorePasswords) {
        if (probeAvailable(currentPulsarServiceUrl)) {
            failedTimestamp = -1;
            return;
        }

        long currentTimestamp = System.nanoTime();
        if (failedTimestamp == -1) {
            failedTimestamp = currentTimestamp;
        } else if (currentTimestamp - failedTimestamp >= failoverDelayNs) {
            for (int i = 0; i < targetServiceUrls.size(); ++i) {
                if (probeAvailable(targetServiceUrls.get(i))) {
                    log.info("Current Pulsar service is {}, it has been down for {} ms, "
                                    + "switch to the service {}. The current service down at {}",
                            currentPulsarServiceUrl, nanosToMillis(currentTimestamp - failedTimestamp),
                            targetServiceUrls.get(i), failedTimestamp);
                    updateServiceUrl(targetServiceUrls.get(i),
                            authentications != null ? authentications.get(i) : null,
                            tlsTrustCertsFilePaths != null ? tlsTrustCertsFilePaths.get(i) : null,
                            tlsTrustStorePaths != null ? tlsTrustStorePaths.get(i) : null,
                            tlsTrustStorePasswords != null ? tlsTrustStorePasswords.get(i) : null);
                    failedTimestamp = -1;
                    break;
                } else {
                    log.warn("Current Pulsar service is {}, it has been down for {} ms. "
                                    + "Failed to switch to service {}, "
                                    + "because it is not available, continue to probe next pulsar service.",
                            currentPulsarServiceUrl, nanosToMillis(currentTimestamp - failedTimestamp),
                            targetServiceUrls.get(i));
                }
            }
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
        private List<String> secondary;
        private Authentication primaryAuthentication = null;
        private List<Authentication> secondaryAuthentication = null;
        private String primaryTlsTrustCertsFilePath = null;
        private List<String> secondaryTlsTrustCertsFilePath = null;
        private String primaryTlsTrustStorePath = null;
        private List<String> secondaryTlsTrustStorePath = null;
        private String primaryTlsTrustStorePassword = null;
        private List<String> secondaryTlsTrustStorePassword = null;
        private long failoverDelayNs;
        private long switchBackDelayNs;
        private long checkIntervalMs = 30_000;

        @Override
        public AutoClusterFailoverBuilder primary(@NonNull String primary) {
            this.primary = primary;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondary(@NonNull List<String> secondary) {
            this.secondary = secondary;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder primaryAuthentication(Authentication authentication) {
            this.primaryAuthentication = authentication;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryAuthentication(List<Authentication> authentication) {
            this.secondaryAuthentication = authentication;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder primaryTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
            this.primaryTlsTrustCertsFilePath = tlsTrustCertsFilePath;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryTlsTrustCertsFilePath(List<String> tlsTrustCertsFilePath) {
            this.secondaryTlsTrustCertsFilePath = tlsTrustCertsFilePath;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder primaryTlsTrustStorePath(String tlsTrustStorePath) {
            this.primaryTlsTrustStorePath = tlsTrustStorePath;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryTlsTrustStorePath(List<String> tlsTrustStorePath) {
            this.secondaryTlsTrustStorePath = tlsTrustStorePath;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder primaryTlsTrustStorePassword(String tlsTrustStorePassword) {
            this.primaryTlsTrustStorePassword = tlsTrustStorePassword;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryTlsTrustStorePassword(List<String> tlsTrustStorePassword) {
            this.secondaryTlsTrustStorePassword = tlsTrustStorePassword;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder failoverDelay(long failoverDelay, TimeUnit timeUnit) {
            this.failoverDelayNs = timeUnit.toNanos(failoverDelay);
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder switchBackDelay(long switchBackDelay, TimeUnit timeUnit) {
            this.switchBackDelayNs = timeUnit.toNanos(switchBackDelay);
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder checkInterval(long interval, TimeUnit timeUnit) {
            this.checkIntervalMs = timeUnit.toMillis(interval);
            return this;
        }

        @Override
        public ServiceUrlProvider build() {
            Objects.requireNonNull(primary, "primary service url shouldn't be null");
            checkArgument(secondary != null && secondary.size() > 0,
                    "secondary cluster service url shouldn't be null and should set at least one");
            checkArgument(failoverDelayNs >= 0, "failoverDelay should >= 0");
            checkArgument(switchBackDelayNs >= 0, "switchBackDelay should >= 0");
            checkArgument(checkIntervalMs >= 0, "checkInterval should >= 0");
            int secondarySize = secondary.size();

            checkArgument(secondaryAuthentication == null
                    || secondaryAuthentication.size() == secondarySize,
                    "secondaryAuthentication should be null or size equal with secondary url size");
            checkArgument(secondaryTlsTrustCertsFilePath == null
                            || secondaryTlsTrustCertsFilePath.size() == secondarySize,
                    "secondaryTlsTrustCertsFilePath should be null or size equal with secondary url size");
            checkArgument(secondaryTlsTrustStorePath == null
                            || secondaryTlsTrustStorePath.size() == secondarySize,
                    "secondaryTlsTrustStorePath should be null or size equal with secondary url size");
            checkArgument(secondaryTlsTrustStorePassword == null
                            || secondaryTlsTrustStorePassword.size() == secondarySize,
                    "secondaryTlsTrustStorePassword should be null or size equal with secondary url size");

            return new AutoClusterFailover(primary, secondary, failoverDelayNs, switchBackDelayNs, checkIntervalMs,
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
