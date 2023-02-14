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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
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
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;

@Slf4j
@Data
public class AutoClusterFailover implements ServiceUrlProvider {
    private PulsarClientImpl pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private final String primary;
    private final List<String> secondary;
    private final AutoClusterFailoverBuilder.FailoverPolicy failoverPolicy;
    private Authentication primaryAuthentication;
    private final Map<String, Authentication> secondaryAuthentications;
    private String primaryTlsTrustCertsFilePath;
    private final Map<String, String> secondaryTlsTrustCertsFilePaths;
    private String primaryTlsTrustStorePath;
    private Map<String, String> secondaryTlsTrustStorePaths;
    private String primaryTlsTrustStorePassword;
    private Map<String, String> secondaryTlsTrustStorePasswords;
    private final long failoverDelayNs;
    private final long switchBackDelayNs;
    private final ScheduledExecutorService executor;
    private long recoverTimestamp;
    private long failedTimestamp;
    private final long intervalMs;
    private static final int TIMEOUT = 30_000;
    private final PulsarServiceNameResolver resolver;

    private AutoClusterFailover(AutoClusterFailoverBuilderImpl builder) {
        this.primary = builder.primary;
        this.secondary = builder.secondary;
        this.failoverPolicy = builder.failoverPolicy;
        this.secondaryAuthentications = builder.secondaryAuthentications;
        this.secondaryTlsTrustCertsFilePaths = builder.secondaryTlsTrustCertsFilePaths;
        this.secondaryTlsTrustStorePaths = builder.secondaryTlsTrustStorePaths;
        this.secondaryTlsTrustStorePasswords = builder.secondaryTlsTrustStorePasswords;
        this.failoverDelayNs = builder.failoverDelayNs;
        this.switchBackDelayNs = builder.switchBackDelayNs;
        this.currentPulsarServiceUrl = builder.primary;
        this.recoverTimestamp = -1;
        this.failedTimestamp = -1;
        this.intervalMs = builder.checkIntervalMs;
        this.resolver = new PulsarServiceNameResolver();
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("pulsar-service-provider"));
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = (PulsarClientImpl) client;
        ClientConfigurationData config = pulsarClient.getConfiguration();
        if (config != null) {
            this.primaryAuthentication = config.getAuthentication();
            this.primaryTlsTrustCertsFilePath = config.getTlsTrustCertsFilePath();
            this.primaryTlsTrustStorePath = config.getTlsTrustStorePath();
            this.primaryTlsTrustStorePassword = config.getTlsTrustStorePassword();
        }

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
                // secondary cluster is up, check whether need to switch back to primary or not
                if (!currentPulsarServiceUrl.equals(primary)) {
                    probeAndCheckSwitchBack(primary, primaryAuthentication, primaryTlsTrustCertsFilePath,
                            primaryTlsTrustStorePath, primaryTlsTrustStorePassword);
                }
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
            resolver.updateServiceUrl(url);
            InetSocketAddress endpoint = resolver.resolveHost();
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(endpoint.getHostName(), endpoint.getPort()), TIMEOUT);
            socket.close();
            return true;
        } catch (Exception e) {
            log.warn("Failed to probe available, url: {}", url, e);
            return false;
        }
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
            pulsarClient.reloadLookUp();
            currentPulsarServiceUrl = target;
        } catch (IOException e) {
            log.error("Current Pulsar service is {}, "
                    + "failed to switch back to {} ", currentPulsarServiceUrl, target, e);
        }
    }

    private void probeAndUpdateServiceUrl(List<String> targetServiceUrls,
                                          Map<String, Authentication> authentications,
                                          Map<String, String> tlsTrustCertsFilePaths,
                                          Map<String, String> tlsTrustStorePaths,
                                          Map<String, String> tlsTrustStorePasswords) {
        if (probeAvailable(currentPulsarServiceUrl)) {
            failedTimestamp = -1;
            return;
        }

        long currentTimestamp = System.nanoTime();
        if (failedTimestamp == -1) {
            failedTimestamp = currentTimestamp;
        } else if (currentTimestamp - failedTimestamp >= failoverDelayNs) {
            for (String targetServiceUrl : targetServiceUrls) {
                if (probeAvailable(targetServiceUrl)) {
                    log.info("Current Pulsar service is {}, it has been down for {} ms, "
                                    + "switch to the service {}. The current service down at {}",
                            currentPulsarServiceUrl, nanosToMillis(currentTimestamp - failedTimestamp),
                            targetServiceUrl, failedTimestamp);
                    updateServiceUrl(targetServiceUrl,
                            authentications != null ? authentications.get(targetServiceUrl) : null,
                            tlsTrustCertsFilePaths != null ? tlsTrustCertsFilePaths.get(targetServiceUrl) : null,
                            tlsTrustStorePaths != null ? tlsTrustStorePaths.get(targetServiceUrl) : null,
                            tlsTrustStorePasswords != null ? tlsTrustStorePasswords.get(targetServiceUrl) : null);
                    failedTimestamp = -1;
                    break;
                } else {
                    log.warn("Current Pulsar service is {}, it has been down for {} ms. "
                                    + "Failed to switch to service {}, "
                                    + "because it is not available, continue to probe next pulsar service.",
                        currentPulsarServiceUrl, nanosToMillis(currentTimestamp - failedTimestamp), targetServiceUrl);
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
        private Map<String, Authentication> secondaryAuthentications = null;
        private Map<String, String> secondaryTlsTrustCertsFilePaths = null;
        private Map<String, String> secondaryTlsTrustStorePaths = null;
        private Map<String, String> secondaryTlsTrustStorePasswords = null;
        private FailoverPolicy failoverPolicy = FailoverPolicy.ORDER;
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
        public AutoClusterFailoverBuilder failoverPolicy(@NonNull FailoverPolicy policy) {
            this.failoverPolicy = policy;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryAuthentication(Map<String, Authentication> authentication) {
            this.secondaryAuthentications = authentication;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryTlsTrustCertsFilePath(Map<String, String> tlsTrustCertsFilePath) {
            this.secondaryTlsTrustCertsFilePaths = tlsTrustCertsFilePath;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryTlsTrustStorePath(Map<String, String> tlsTrustStorePath) {
            this.secondaryTlsTrustStorePaths = tlsTrustStorePath;
            return this;
        }

        @Override
        public AutoClusterFailoverBuilder secondaryTlsTrustStorePassword(Map<String, String> tlsTrustStorePassword) {
            this.secondaryTlsTrustStorePasswords = tlsTrustStorePassword;
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
            checkArgument(failoverDelayNs > 0, "failoverDelay should > 0");
            checkArgument(switchBackDelayNs > 0, "switchBackDelay should > 0");
            checkArgument(checkIntervalMs > 0, "checkInterval should > 0");
            int secondarySize = secondary.size();

            checkArgument(secondaryAuthentications == null
                    || secondaryAuthentications.size() == secondarySize,
                    "secondaryAuthentication should be null or size equal with secondary url size");
            checkArgument(secondaryTlsTrustCertsFilePaths == null
                            || secondaryTlsTrustCertsFilePaths.size() == secondarySize,
                    "secondaryTlsTrustCertsFilePath should be null or size equal with secondary url size");
            checkArgument(secondaryTlsTrustStorePaths == null
                            || secondaryTlsTrustStorePaths.size() == secondarySize,
                    "secondaryTlsTrustStorePath should be null or size equal with secondary url size");
            checkArgument(secondaryTlsTrustStorePasswords == null
                            || secondaryTlsTrustStorePasswords.size() == secondarySize,
                    "secondaryTlsTrustStorePassword should be null or size equal with secondary url size");

            return new AutoClusterFailover(this);
        }

        public static void checkArgument(boolean expression, @NonNull Object errorMessage) {
            if (!expression) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
        }
    }

    public static AutoClusterFailoverBuilder builder() {
        return new AutoClusterFailoverBuilderImpl();
    }
}
