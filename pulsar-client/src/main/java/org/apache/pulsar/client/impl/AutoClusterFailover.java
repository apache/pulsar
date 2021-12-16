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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AutoClusterFailoverBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;

@Slf4j
@Data
public class AutoClusterFailover implements ServiceUrlProvider {
    private PulsarClient pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private final String primary;
    private final String secondary;
    private final long failoverDelayNs;
    private final long switchBackDelayNs;
    private final ScheduledExecutorService executor;
    private long recoverTimestamp;
    private long failedTimestamp;
    private final int TIMEOUT = 30_000;

    private AutoClusterFailover(String primary, String secondary, long failoverDelayNs, long switchBackDelayNs) {
        this.primary = primary;
        this.secondary = secondary;
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
                probeAndUpdateServiceUrl(secondary);
            } else {
                // current service url is secondary, probe whether it is down
                probeAndUpdateServiceUrl(primary);
                // secondary cluster is up, check whether need to switch back to primary
                probeAndCheckSwitchBack(primary);
            }
        }), 30_000, 30_000, TimeUnit.MILLISECONDS);

    }

    @Override
    public String getServiceUrl() {
        return this.currentPulsarServiceUrl;
    }

    @Override
    public void close() {
        this.executor.shutdown();
    }

    private boolean probeAvailable(String url) {
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
            log.error("Failed to probe available, url: {}", url, e);
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

    private static Long Ns2Ms(long timeStampNs) {
        return timeStampNs / 1000000;
    }

    private void updateServiceUrl(String target) {
        try {
            pulsarClient.updateServiceUrl(target);
            currentPulsarServiceUrl = target;
        } catch (PulsarClientException e) {
            log.error("Current Pulsar service is {}, "
                    + "failed to switch back to {} ", currentPulsarServiceUrl, target, e);
        }
    }

    private void probeAndUpdateServiceUrl(String targetServiceUrl) {
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
                                + "switch to the service: {}. The current service down at: {}",
                        currentPulsarServiceUrl, Ns2Ms(currentTimestamp - failedTimestamp),
                        targetServiceUrl, Ns2Ms(failedTimestamp));
                updateServiceUrl(targetServiceUrl);
                failedTimestamp = -1;
            } else {
                log.error("Current Pulsar service is {}, it has been down for {} ms. "
                                + "Failed to switch to service {}, "
                                + "because it is not available",
                        currentPulsarServiceUrl, Ns2Ms(currentTimestamp - failedTimestamp),
                        targetServiceUrl);
            }
        }
    }

    private void probeAndCheckSwitchBack(String target) {
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
                    currentPulsarServiceUrl, target, Ns2Ms(currentTimestamp - recoverTimestamp));
            updateServiceUrl(target);
            recoverTimestamp = -1;
        }
    }

    public static class AutoClusterFailoverBuilderImpl implements AutoClusterFailoverBuilder {
        private String primary;
        private String secondary;
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

        public AutoClusterFailoverBuilder failoverDelay(long failoverDelay, TimeUnit timeUnit) {
            this.failoverDelayNs = timeUnit.toNanos(failoverDelay);
            return this;
        }

        public AutoClusterFailoverBuilder switchBackDelay(long switchBackDelay, TimeUnit timeUnit) {
            this.switchBackDelayNs = timeUnit.toNanos(switchBackDelay);
            return this;
        }

        public ServiceUrlProvider build() {
            return new AutoClusterFailover(primary, secondary, failoverDelayNs, switchBackDelayNs);
        }
    }

    public static AutoClusterFailoverBuilder builder() {
        return new AutoClusterFailoverBuilderImpl();
    }
}
