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
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.common.util.ObjectMapperFactory.getThreadLocal;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.proxy.server.util.ZookeeperCacheLoader;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Maintains available active broker list and returns next active broker in round-robin for discovery service.
 *
 */
public class BrokerDiscoveryProvider implements Closeable {

    final ZookeeperCacheLoader localZkCache;
    final GlobalZooKeeperCache globalZkCache;
    private final AtomicInteger counter = new AtomicInteger();

    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(4)
            .name("pulsar-proxy-ordered").build();
    private final ScheduledExecutorService scheduledExecutorScheduler = Executors.newScheduledThreadPool(4,
            new DefaultThreadFactory("pulsar-proxy-scheduled-executor"));

    private static final String PARTITIONED_TOPIC_PATH_ZNODE = "partitioned-topics";

    public BrokerDiscoveryProvider(ProxyConfiguration config, ZooKeeperClientFactory zkClientFactory)
            throws PulsarServerException {
        try {
            localZkCache = new ZookeeperCacheLoader(zkClientFactory, config.getZookeeperServers(),
                    config.getZookeeperSessionTimeoutMs());
            globalZkCache = new GlobalZooKeeperCache(zkClientFactory, config.getZookeeperSessionTimeoutMs(),
                    (int) TimeUnit.MILLISECONDS.toSeconds(config.getZookeeperSessionTimeoutMs()),
                    config.getConfigurationStoreServers(), orderedExecutor, scheduledExecutorScheduler,
                    config.getZookeeperSessionTimeoutMs());
            globalZkCache.start();
        } catch (Exception e) {
            LOG.error("Failed to start ZooKeeper {}", e.getMessage(), e);
            throw new PulsarServerException("Failed to start zookeeper :" + e.getMessage(), e);
        }
    }

    /**
     * Find next broker {@link LoadManagerReport} in round-robin fashion.
     *
     * @return
     * @throws PulsarServerException
     */
    LoadManagerReport nextBroker() throws PulsarServerException {
        List<LoadManagerReport> availableBrokers = localZkCache.getAvailableBrokers();

        if (availableBrokers.isEmpty()) {
            throw new PulsarServerException("No active broker is available");
        } else {
            int brokersCount = availableBrokers.size();
            int nextIdx = signSafeMod(counter.getAndIncrement(), brokersCount);
            return availableBrokers.get(nextIdx);
        }
    }

    CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(ProxyService service,
            TopicName topicName, String role, AuthenticationDataSource authenticationData) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        try {
            checkAuthorization(service, topicName, role, authenticationData);
            final String path = path(PARTITIONED_TOPIC_PATH_ZNODE,
                    topicName.getNamespaceObject().toString(), "persistent", topicName.getEncodedLocalName());
            // gets the number of partitions from the zk cache
            globalZkCache
                    .getDataAsync(path,
                            (key, content) -> getThreadLocal().readValue(content, PartitionedTopicMetadata.class))
                    .thenAccept(metadata -> {
                        // if the partitioned topic is not found in zk, then the topic
                        // is not partitioned
                        if (metadata.isPresent()) {
                            metadataFuture.complete(metadata.get());
                        } else {
                            metadataFuture.complete(new PartitionedTopicMetadata());
                        }
                    }).exceptionally(ex -> {
                        metadataFuture.completeExceptionally(ex);
                        return null;
                    });
        } catch (Exception e) {
            metadataFuture.completeExceptionally(e);
        }
        return metadataFuture;
    }

    protected static void checkAuthorization(ProxyService service, TopicName topicName, String role,
            AuthenticationDataSource authenticationData) throws Exception {
        if (!service.getConfiguration().isAuthorizationEnabled()
                || service.getConfiguration().getSuperUserRoles().contains(role)) {
            // No enforcing of authorization policies
            return;
        }
        // get zk policy manager
        if (!service.getAuthorizationService().canLookup(topicName, role, authenticationData)) {
            LOG.warn("[{}] Role {} is not allowed to lookup topic", topicName, role);
            // check namespace authorization
            TenantInfo tenantInfo;
            try {
                tenantInfo = service.getConfigurationCacheService().propertiesCache()
                        .get(path(POLICIES, topicName.getTenant()))
                        .orElseThrow(() -> new IllegalAccessException("Property does not exist"));
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("Failed to get property admin data for non existing property {}", topicName.getTenant());
                throw new IllegalAccessException("Property does not exist");
            } catch (Exception e) {
                LOG.error("Failed to get property admin data for property");
                throw new IllegalAccessException(String.format("Failed to get property %s admin data due to %s",
                        topicName.getTenant(), e.getMessage()));
            }
            if (!service.getAuthorizationService().isTenantAdmin(topicName.getTenant(), role, tenantInfo, authenticationData).get()) {
                throw new IllegalAccessException("Don't have permission to administrate resources on this tenant");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully authorized {} on property {}", role, topicName.getTenant());
        }
    }

    public static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        localZkCache.close();
        globalZkCache.close();
        orderedExecutor.shutdown();
        scheduledExecutorScheduler.shutdownNow();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BrokerDiscoveryProvider.class);

}
