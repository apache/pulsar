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

import com.google.common.base.Strings;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;

@Slf4j
public class AutoClusterFailover implements ServiceUrlProvider {
    private PulsarClient pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private final String primary;
    private final String secondary;
    private final long failoverDelayMs;
    private final long switchBackDelayMs;
    private final Timer timer;
    private volatile long primaryFailedTimestamp;
    private long primaryRecoverTimestamp;
    private long secondaryFailedTimestamp;
    private final int timeout = 30_000;

    private AutoClusterFailover(String primary, String secondary, long failoverDelayMs, long switchBackDelayMs) {
        this.primary = primary;
        this.secondary = secondary;
        this.failoverDelayMs = failoverDelayMs;
        this.switchBackDelayMs = switchBackDelayMs;
        this.primaryFailedTimestamp = -1;
        this.primaryRecoverTimestamp = -1;
        this.secondaryFailedTimestamp = -1;
        this.timer = new Timer("pulsar-service-provider");
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = client;
        this.currentPulsarServiceUrl = primary;

        // start to probe primary cluster active or not
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                // current pulsar serviceUrl is primary
                if (currentPulsarServiceUrl.equals(primary)) {
                    if (probeAvailable(primary, timeout)) {
                        primaryFailedTimestamp = -1;
                        return;
                    }

                    if (primaryFailedTimestamp == -1) {
                        primaryFailedTimestamp = System.currentTimeMillis();
                    } else if (System.currentTimeMillis() - primaryFailedTimestamp < failoverDelayMs) {
                        return;
                    } else if (probeAvailable(secondary, timeout)){
                        log.info("Current Pulsar service is primary: {}, it has been down for {} ms, "
                                        + "switch to the secondary service: {}. The first primary service down at: {}",
                                currentPulsarServiceUrl, System.currentTimeMillis() - primaryFailedTimestamp,
                                secondary, primaryFailedTimestamp);
                        try {
                            pulsarClient.updateServiceUrl(secondary);
                            currentPulsarServiceUrl = secondary;
                        } catch (PulsarClientException e) {
                            log.error("Failed to switch to secondary service URL ", e);
                        }
                    } else {
                        log.error("Current Pulsar service is primary: {}, it has been down for {} ms. "
                                + "Failed to switch to secondary service URL, "
                                + "because secondary service URL is not available",
                                currentPulsarServiceUrl, System.currentTimeMillis() - primaryFailedTimestamp);
                    }
                } else { // current pulsar service URL is secondary, probe whether we need to switch back to primary.
                    if (!probeAvailable(currentPulsarServiceUrl, timeout)) {
                        if (secondaryFailedTimestamp == -1) {
                            secondaryFailedTimestamp = System.currentTimeMillis();
                        } else if (System.currentTimeMillis() - secondaryFailedTimestamp >= failoverDelayMs
                                && probeAvailable(primary, timeout)) {
                            log.info("Current Pulsar service is secondary: {}, it has been down for {} ms, "
                                    + "switch back to primary service: {}", currentPulsarServiceUrl,
                                    System.currentTimeMillis() - secondaryFailedTimestamp, primary);
                            try {
                                pulsarClient.updateServiceUrl(primary);
                                currentPulsarServiceUrl = primary;
                                return;
                            } catch (PulsarClientException e) {
                                log.error("Current Pulsar service is secondary: {}, it has been down for {} ms. "
                                        + "Failed to switch to secondary service URL ",
                                        currentPulsarServiceUrl,
                                        System.currentTimeMillis() - secondaryFailedTimestamp, e);
                            }
                        }

                        return;
                    }

                    secondaryFailedTimestamp = -1;

                    if (!probeAvailable(primary, timeout)) {
                        primaryRecoverTimestamp = -1;
                        return;
                    }
                    if (primaryRecoverTimestamp == -1) {
                        primaryRecoverTimestamp = System.currentTimeMillis();
                    } else if (System.currentTimeMillis() - primaryRecoverTimestamp >= switchBackDelayMs) {
                        log.info("Current Pulsar service is secondary: {}, "
                                        + "the primary service: {} has been recover for {} ms, "
                                        + "switch back to the primary service",
                                currentPulsarServiceUrl, primary, System.currentTimeMillis() - primaryRecoverTimestamp);
                        try {
                            pulsarClient.updateServiceUrl(primary);
                            currentPulsarServiceUrl = primary;
                        } catch (PulsarClientException e) {
                            log.error("Current Pulsar service is secondary: {}, "
                                    + "failed to switch back to primary service URL ", currentPulsarServiceUrl, e);
                        }
                    }
                }
            }
        }, 30_000, 30_000);

    }

    @Override
    public String getServiceUrl() {
        return this.currentPulsarServiceUrl;
    }

    @Override
    public void close() {
        this.timer.cancel();
    }

    private boolean probeAvailable(String url, int timeout) {
        try {
            String hostAndPort = parseHostAndPort(url);
            if (Strings.isNullOrEmpty(hostAndPort)) {
                return false;
            }

            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(parseHost(hostAndPort), parsePort(hostAndPort)), timeout);
            socket.close();
            return true;
        } catch (Exception e) {
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

    public static class Builder {
        private String primary;
        private String secondary;
        private long failoverDelayMs;
        private long switchBackDelayMs;


        public Builder primary(String primary) {
            this.primary = primary;
            return this;
        }

        public Builder secondary(String secondary) {
            this.secondary = secondary;
            return this;
        }

        public Builder failoverDelay(int failoverDelay, TimeUnit failoverDelayTimeUnit) {
            this.failoverDelayMs = failoverDelayTimeUnit.toMillis(failoverDelay);
            return this;
        }

        public Builder switchBackDelay(int switchBackDelay, TimeUnit switchBackDelayTimeUnit) {
            this.switchBackDelayMs = switchBackDelayTimeUnit.toMillis(switchBackDelay);
            return this;
        }

        public AutoClusterFailover build() {
            return new AutoClusterFailover(primary, secondary, failoverDelayMs, switchBackDelayMs);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
