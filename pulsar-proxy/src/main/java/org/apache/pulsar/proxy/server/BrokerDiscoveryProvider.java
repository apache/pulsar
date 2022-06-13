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
package org.apache.pulsar.proxy.server;

import static org.apache.bookkeeper.common.util.MathUtils.signSafeMod;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.resources.MetadataStoreCacheLoader;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains available active broker list and returns next active broker in round-robin for discovery service.
 * This is an API used by Proxy Extensions.
 */
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate
public class BrokerDiscoveryProvider implements Closeable {

    final MetadataStoreCacheLoader metadataStoreCacheLoader;
    final PulsarResources pulsarResources;

    private final AtomicInteger counter = new AtomicInteger();

    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(4)
            .name("pulsar-proxy-ordered").build();
    private final ScheduledExecutorService scheduledExecutorScheduler = Executors.newScheduledThreadPool(4,
            new DefaultThreadFactory("pulsar-proxy-scheduled-executor"));

    public BrokerDiscoveryProvider(ProxyConfiguration config, PulsarResources pulsarResources)
            throws PulsarServerException {
        try {
            this.pulsarResources = pulsarResources;
            this.metadataStoreCacheLoader = new MetadataStoreCacheLoader(pulsarResources,
                    config.getMetadataStoreSessionTimeoutMillis());
        } catch (Exception e) {
            LOG.error("Failed to start ZooKeeper {}", e.getMessage(), e);
            throw new PulsarServerException("Failed to start zookeeper :" + e.getMessage(), e);
        }
    }

    /**
     * Access the list of available brokers.
     * Used by Protocol Handlers
     * @return the list of available brokers
     * @throws PulsarServerException
     */
    public List<? extends ServiceLookupData> getAvailableBrokers() throws PulsarServerException {
        return metadataStoreCacheLoader.getAvailableBrokers();
    }

    /**
     * Find next broker {@link LoadManagerReport} in round-robin fashion.
     *
     * @return
     * @throws PulsarServerException
     */
    LoadManagerReport nextBroker() throws PulsarServerException {
        List<LoadManagerReport> availableBrokers = metadataStoreCacheLoader.getAvailableBrokers();

        if (availableBrokers.isEmpty()) {
            throw new PulsarServerException("No active broker is available");
        } else {
            int brokersCount = availableBrokers.size();
            int nextIdx = signSafeMod(counter.getAndIncrement(), brokersCount);
            return availableBrokers.get(nextIdx);
        }
    }

    @Override
    public void close() throws IOException {
        metadataStoreCacheLoader.close();
        orderedExecutor.shutdown();
        scheduledExecutorScheduler.shutdownNow();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BrokerDiscoveryProvider.class);

}
