/*
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

@Slf4j
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class SameAuthParamsLookupAutoClusterFailover implements ServiceUrlProvider {

    private PulsarClientImpl pulsarClient;
    private EventLoopGroup executor;
    private volatile boolean closed;
    private ScheduledFuture<?> scheduledCheckTask;
    @Getter
    private int failoverThreshold = 5;
    @Getter
    private int recoverThreshold = 5;
    @Getter
    private long checkHealthyIntervalMs = 1000;
    @Getter
    private boolean markTopicNotFoundAsAvailable = true;
    @Getter
    private String testTopic = "public/default/tp_test";

    private String[] pulsarServiceUrlArray;
    private PulsarServiceState[] pulsarServiceStateArray;
    private MutableInt[] checkCounterArray;
    @Getter
    private volatile int currentPulsarServiceIndex;

    private SameAuthParamsLookupAutoClusterFailover() {}

    @Override
    public void initialize(PulsarClient client) {
        this.currentPulsarServiceIndex = 0;
        this.pulsarClient = (PulsarClientImpl) client;
        this.executor = EventLoopUtil.newEventLoopGroup(1, false,
                new ExecutorProvider.ExtendedThreadFactory("broker-service-url-check"));
        scheduledCheckTask = executor.scheduleAtFixedRate(() -> {
            if (closed) {
                return;
            }
            checkPulsarServices();
            int firstHealthyPulsarService = firstHealthyPulsarService();
            if (firstHealthyPulsarService == currentPulsarServiceIndex) {
                return;
            }
            if (firstHealthyPulsarService < 0) {
                int failoverTo = findFailoverTo();
                if (failoverTo < 0) {
                    // No healthy pulsar service to connect.
                    log.error("Failed to choose a pulsar service to connect, no one pulsar service is healthy. Current"
                            + " pulsar service: [{}] {}. States: {}, Counters: {}", currentPulsarServiceIndex,
                            pulsarServiceUrlArray[currentPulsarServiceIndex], Arrays.toString(pulsarServiceStateArray),
                            Arrays.toString(checkCounterArray));
                } else {
                    // Failover to low priority pulsar service.
                    updateServiceUrl(failoverTo);
                }
            } else {
                // Back to high priority pulsar service.
                updateServiceUrl(firstHealthyPulsarService);
            }
        }, checkHealthyIntervalMs, checkHealthyIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceUrl() {
        return pulsarServiceUrlArray[currentPulsarServiceIndex];
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        log.info("Closing service url provider. Current pulsar service: [{}] {}", currentPulsarServiceIndex,
                pulsarServiceUrlArray[currentPulsarServiceIndex]);
        if (scheduledCheckTask != null) {
            scheduledCheckTask.cancel(false);
        }

        if (executor != null) {
            executor.shutdownNow();
        }

        closed = true;
    }

    private int firstHealthyPulsarService() {
        for (int i = 0; i <= currentPulsarServiceIndex; i++) {
            if (pulsarServiceStateArray[i] == PulsarServiceState.Healthy
                    || pulsarServiceStateArray[i] == PulsarServiceState.PreFail) {
                return i;
            }
        }
        return -1;
    }

    private int findFailoverTo() {
        for (int i = currentPulsarServiceIndex + 1; i <= pulsarServiceUrlArray.length; i++) {
            if (probeAvailable(i)) {
                return i;
            }
        }
        return -1;
    }

    private void checkPulsarServices() {
        for (int i = 0; i <= currentPulsarServiceIndex; i++) {
            if (probeAvailable(i)) {
                switch (pulsarServiceStateArray[i]) {
                    case Healthy: {
                        break;
                    }
                    case PreFail: {
                        pulsarServiceStateArray[i] = PulsarServiceState.Healthy;
                        checkCounterArray[i].setValue(0);
                        break;
                    }
                    case Failed: {
                        pulsarServiceStateArray[i] = PulsarServiceState.PreRecover;
                        checkCounterArray[i].setValue(1);
                        break;
                    }
                    case PreRecover: {
                        checkCounterArray[i].setValue(checkCounterArray[i].getValue() + 1);
                        if (checkCounterArray[i].getValue() >= recoverThreshold) {
                            pulsarServiceStateArray[i] = PulsarServiceState.Healthy;
                            checkCounterArray[i].setValue(0);
                        }
                        break;
                    }
                }
            } else {
                switch (pulsarServiceStateArray[i]) {
                    case Healthy: {
                        pulsarServiceStateArray[i] = PulsarServiceState.PreFail;
                        checkCounterArray[i].setValue(1);
                        break;
                    }
                    case PreFail: {
                        checkCounterArray[i].setValue(checkCounterArray[i].getValue() + 1);
                        if (checkCounterArray[i].getValue() >= failoverThreshold) {
                            pulsarServiceStateArray[i] = PulsarServiceState.Failed;
                            checkCounterArray[i].setValue(0);
                        }
                        break;
                    }
                    case Failed: {
                        break;
                    }
                    case PreRecover: {
                        pulsarServiceStateArray[i] = PulsarServiceState.Failed;
                        checkCounterArray[i].setValue(0);
                        break;
                    }
                }
            }
        }
    }

    private boolean probeAvailable(int brokerServiceIndex) {
        String url = pulsarServiceUrlArray[brokerServiceIndex];
        try {
            Pair<InetSocketAddress, InetSocketAddress> res =
                    pulsarClient.getLookup(url).getBroker(TopicName.get(testTopic)).get(3, TimeUnit.SECONDS);
            if (log.isDebugEnabled()) {
                log.debug("Success to probe available(lookup res: {}), [{}] {}}. States: {}, Counters: {}",
                        res.toString(), brokerServiceIndex, url, Arrays.toString(pulsarServiceStateArray),
                        Arrays.toString(checkCounterArray));
            }
            return true;
        } catch (Exception e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            if (actEx instanceof PulsarAdminException.NotFoundException
                    || actEx instanceof PulsarClientException.NotFoundException
                    || actEx instanceof PulsarClientException.TopicDoesNotExistException
                    || actEx instanceof PulsarClientException.LookupException) {
                if (markTopicNotFoundAsAvailable) {
                    if (log.isDebugEnabled()) {
                        log.debug("Success to probe available(case tenant/namespace/topic not found), [{}] {}."
                                + " States: {}, Counters: {}", brokerServiceIndex, url,
                                Arrays.toString(pulsarServiceStateArray), Arrays.toString(checkCounterArray));
                    }
                    return true;
                } else {
                    log.warn("Failed to probe available(error tenant/namespace/topic not found), [{}] {}. States: {},"
                            + " Counters: {}", brokerServiceIndex, url, Arrays.toString(pulsarServiceStateArray),
                            Arrays.toString(checkCounterArray));
                    return false;
                }
            }
            log.warn("Failed to probe available, [{}] {}. States: {}, Counters: {}", brokerServiceIndex, url,
                    Arrays.toString(pulsarServiceStateArray), Arrays.toString(checkCounterArray));
            return false;
        }
    }

    private void updateServiceUrl(int targetIndex) {
        String currentUrl = pulsarServiceUrlArray[currentPulsarServiceIndex];
        String targetUrl = pulsarServiceUrlArray[targetIndex];
        String logMsg;
        if (targetIndex < currentPulsarServiceIndex) {
            logMsg = String.format("Recover to high priority pulsar service [%s] %s --> [%s] %s. States: %s,"
                    + " Counters: %s", currentPulsarServiceIndex, currentUrl, targetIndex, targetUrl,
                    Arrays.toString(pulsarServiceStateArray), Arrays.toString(checkCounterArray));
        } else {
            logMsg = String.format("Failover to low priority pulsar service [%s] %s --> [%s] %s. States: %s,"
                    + " Counters: %s", currentPulsarServiceIndex, currentUrl, targetIndex, targetUrl,
                    Arrays.toString(pulsarServiceStateArray), Arrays.toString(checkCounterArray));
        }
        log.info(logMsg);
        try {
            pulsarClient.updateServiceUrl(targetUrl);
            pulsarClient.reloadLookUp();
            currentPulsarServiceIndex = targetIndex;
        } catch (Exception e) {
            log.error("Failed to {}", logMsg, e);
        }
    }

    public enum PulsarServiceState {
        Healthy,
        PreFail,
        Failed,
        PreRecover;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private SameAuthParamsLookupAutoClusterFailover
                sameAuthParamsLookupAutoClusterFailover = new SameAuthParamsLookupAutoClusterFailover();

        public Builder failoverThreshold(int failoverThreshold) {
            if (failoverThreshold < 1) {
                throw new IllegalArgumentException("failoverThreshold must be larger than 0");
            }
            sameAuthParamsLookupAutoClusterFailover.failoverThreshold = failoverThreshold;
            return this;
        }

        public Builder recoverThreshold(int recoverThreshold) {
            if (recoverThreshold < 1) {
                throw new IllegalArgumentException("recoverThreshold must be larger than 0");
            }
            sameAuthParamsLookupAutoClusterFailover.recoverThreshold = recoverThreshold;
            return this;
        }

        public Builder checkHealthyIntervalMs(int checkHealthyIntervalMs) {
            if (checkHealthyIntervalMs < 1) {
                throw new IllegalArgumentException("checkHealthyIntervalMs must be larger than 0");
            }
            sameAuthParamsLookupAutoClusterFailover.checkHealthyIntervalMs = checkHealthyIntervalMs;
            return this;
        }

        public Builder testTopic(String testTopic) {
            if (StringUtils.isBlank(testTopic) && TopicName.get(testTopic) != null) {
                throw new IllegalArgumentException("testTopic can not be blank");
            }
            sameAuthParamsLookupAutoClusterFailover.testTopic = testTopic;
            return this;
        }

        public Builder markTopicNotFoundAsAvailable(boolean markTopicNotFoundAsAvailable) {
            sameAuthParamsLookupAutoClusterFailover.markTopicNotFoundAsAvailable = markTopicNotFoundAsAvailable;
            return this;
        }

        public Builder pulsarServiceUrlArray(String[] pulsarServiceUrlArray) {
            if (pulsarServiceUrlArray == null || pulsarServiceUrlArray.length == 0) {
                throw new IllegalArgumentException("pulsarServiceUrlArray can not be empty");
            }
            sameAuthParamsLookupAutoClusterFailover.pulsarServiceUrlArray = pulsarServiceUrlArray;
            int pulsarServiceLen = pulsarServiceUrlArray.length;
            HashSet<String> uniqueChecker = new HashSet<>();
            for (int i = 0; i < pulsarServiceLen; i++) {
                String pulsarService = pulsarServiceUrlArray[i];
                if (StringUtils.isBlank(pulsarService)) {
                    throw new IllegalArgumentException("pulsarServiceUrlArray contains a blank value at index " + i);
                }
                if (pulsarService.startsWith("http") || pulsarService.startsWith("HTTP")) {
                    throw new IllegalArgumentException("SameAuthParamsLookupAutoClusterFailover does not support HTTP"
                            + " protocol pulsar service url so far.");
                }
                if (!uniqueChecker.add(pulsarService)) {
                    throw new IllegalArgumentException("pulsarServiceUrlArray contains duplicated value "
                            + pulsarServiceUrlArray[i]);
                }
            }
            return this;
        }

        public SameAuthParamsLookupAutoClusterFailover build() {
            String[] pulsarServiceUrlArray = sameAuthParamsLookupAutoClusterFailover.pulsarServiceUrlArray;
            if (pulsarServiceUrlArray == null) {
                throw new IllegalArgumentException("pulsarServiceUrlArray can not be empty");
            }
            int pulsarServiceLen = pulsarServiceUrlArray.length;
            sameAuthParamsLookupAutoClusterFailover.pulsarServiceStateArray = new PulsarServiceState[pulsarServiceLen];
            sameAuthParamsLookupAutoClusterFailover.checkCounterArray = new MutableInt[pulsarServiceLen];
            for (int i = 0; i < pulsarServiceLen; i++) {
                sameAuthParamsLookupAutoClusterFailover.pulsarServiceStateArray[i] = PulsarServiceState.Healthy;
                sameAuthParamsLookupAutoClusterFailover.checkCounterArray[i] = new MutableInt(0);
            }
            return sameAuthParamsLookupAutoClusterFailover;
        }
    }
}

