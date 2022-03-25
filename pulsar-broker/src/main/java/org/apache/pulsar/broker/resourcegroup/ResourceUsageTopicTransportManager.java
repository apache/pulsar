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
package org.apache.pulsar.broker.resourcegroup;

import static org.apache.pulsar.client.api.CompressionType.LZ4;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsageInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource Usage Transport Manager
 *
 * <P>Module to exchange usage information with other brokers. Implements a task to periodically.
 * <P>publish the usage as well as handlers to process the usage info from other brokers.
 *
 * @see <a href="https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting">Global-quotas</a>
 *
 */
public class ResourceUsageTopicTransportManager implements ResourceUsageTransportManager {

    private class ResourceUsageWriterTask implements Runnable, AutoCloseable {
        private final Producer<ByteBuffer> producer;
        private final ScheduledFuture<?> resourceUsagePublishTask;

        private Producer<ByteBuffer> createProducer() throws PulsarClientException {
            final int publishDelayMilliSecs = 10;
            final int sendTimeoutSecs = 10;

            return pulsarClient.newProducer(Schema.BYTEBUFFER)
                    .topic(RESOURCE_USAGE_TOPIC_NAME)
                    .batchingMaxPublishDelay(publishDelayMilliSecs, TimeUnit.MILLISECONDS)
                    .sendTimeout(sendTimeoutSecs, TimeUnit.SECONDS)
                    .blockIfQueueFull(false)
                    .compressionType(LZ4)
                    .create();
        }

        public ResourceUsageWriterTask() throws PulsarClientException {
            producer = createProducer();
            resourceUsagePublishTask = pulsarService.getExecutor().scheduleAtFixedRate(
                    catchingAndLoggingThrowables(this),
                    pulsarService.getConfig().getResourceUsageTransportPublishIntervalInSecs(),
                    pulsarService.getConfig().getResourceUsageTransportPublishIntervalInSecs(),
                    TimeUnit.SECONDS);
        }

        @Override
        public synchronized void run() {
            if (resourceUsagePublishTask.isCancelled()) {
                return;
            }
            if (!publisherMap.isEmpty()) {
                ResourceUsageInfo rUsageInfo = new ResourceUsageInfo();
                rUsageInfo.setBroker(pulsarService.getBrokerServiceUrl());

                publisherMap.forEach((key, item) -> item.fillResourceUsage(rUsageInfo.addUsageMap()));

                ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer(rUsageInfo.getSerializedSize());
                rUsageInfo.writeTo(buf);

                producer.sendAsync(buf.nioBuffer()).whenComplete((id, ex) -> {
                    if (null != ex) {
                        LOG.error("Resource usage publisher: error sending message ID {}", id, ex);
                    }
                    buf.release();
                });
            }
        }

        @Override
        public synchronized void close() throws Exception {
            resourceUsagePublishTask.cancel(true);
            producer.close();
        }
    }

    private class ResourceUsageReader implements ReaderListener<byte[]>, AutoCloseable {
        private final ResourceUsageInfo recdUsageInfo = new ResourceUsageInfo();

        private final Reader<byte[]> consumer;

        public ResourceUsageReader() throws PulsarClientException {
            consumer =  pulsarClient.newReader()
                    .topic(RESOURCE_USAGE_TOPIC_NAME)
                    .startMessageId(MessageId.latest)
                    .readerListener(this)
                    .create();
            }

        @Override
        public void close() throws Exception {
            consumer.close();
        }

        @Override
        public void received(Reader<byte[]> reader, Message<byte[]> msg) {
            long publishTime = msg.getPublishTime();
            long currentTime = System.currentTimeMillis();
            long timeDelta = currentTime - publishTime;

            recdUsageInfo.parseFrom(Unpooled.wrappedBuffer(msg.getData()), msg.getData().length);
            if (timeDelta > TimeUnit.SECONDS.toMillis(
            2 * pulsarService.getConfig().getResourceUsageTransportPublishIntervalInSecs())) {
                LOG.error("Stale resource usage msg from broker {} publish time {} current time{}",
                recdUsageInfo.getBroker(), publishTime, currentTime);
                return;
            }
            try {
                recdUsageInfo.getUsageMapsList().forEach(ru -> {
                    ResourceUsageConsumer owner = consumerMap.get(ru.getOwner());
                    if (owner != null) {
                        owner.acceptResourceUsage(recdUsageInfo.getBroker(), ru);
                    }
                });

            } catch (IllegalStateException exception) {
                LOG.error("Resource usage reader: Error parsing incoming message", exception);
            } catch (Exception exception) {
                LOG.error("Resource usage reader: Unknown exception while parsing message", exception);
            }
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(ResourceUsageTopicTransportManager.class);
    public  static final String RESOURCE_USAGE_TOPIC_NAME = "non-persistent://pulsar/system/resource-usage";
    private final PulsarService pulsarService;
    private final PulsarClient pulsarClient;
    private final ResourceUsageWriterTask pTask;
    private final ResourceUsageReader consumer;
    private final Map<String, ResourceUsagePublisher>
            publisherMap = new ConcurrentHashMap<String, ResourceUsagePublisher>();
    private final Map<String, ResourceUsageConsumer>
            consumerMap = new ConcurrentHashMap<String, ResourceUsageConsumer>();

    private void createTenantAndNamespace() throws PulsarServerException, PulsarAdminException {
        // Create a public tenant and default namespace
        TopicName topicName = TopicName.get(RESOURCE_USAGE_TOPIC_NAME);

        PulsarAdmin admin = pulsarService.getAdminClient();
        ServiceConfiguration config = pulsarService.getConfig();
        String cluster = config.getClusterName();

        final String tenant = topicName.getTenant();
        final String namespace = topicName.getNamespace();

        List<String> tenantList =  admin.tenants().getTenants();
        if (!tenantList.contains(tenant)) {
            try {
                admin.tenants().createTenant(tenant,
                  new TenantInfoImpl(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            } catch (PulsarAdminException ex1) {
                if (!(ex1 instanceof PulsarAdminException.ConflictException)) {
                    LOG.error("Unexpected exception {} when creating tenant {}", ex1, tenant);
                    throw ex1;
                }
            }
        }
        List<String> nsList = admin.namespaces().getNamespaces(tenant);
        if (!nsList.contains(namespace)) {
            try {
                admin.namespaces().createNamespace(namespace);
            } catch (PulsarAdminException ex1) {
                if (!(ex1 instanceof PulsarAdminException.ConflictException)) {
                    LOG.error("Unexpected exception {} when creating namespace {}", ex1, namespace);
                    throw ex1;
                }
            }
        }
    }

    public ResourceUsageTopicTransportManager(PulsarService pulsarService)
        throws PulsarServerException, PulsarAdminException, PulsarClientException {
        this.pulsarService = pulsarService;
        this.pulsarClient = pulsarService.getClient();

        try {
            createTenantAndNamespace();
            consumer = new ResourceUsageReader();
            pTask = new ResourceUsageWriterTask();
        } catch (Exception ex) {
            LOG.error("Error initializing resource usage transport manager", ex);
            throw ex;
        }
    }

    /*
     * Register a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage publisher
     */
    public void registerResourceUsagePublisher(ResourceUsagePublisher r) {
        publisherMap.put(r.getID(), r);
    }

    /*
     * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage publisher
     */
    public void unregisterResourceUsagePublisher(ResourceUsagePublisher r) {
        publisherMap.remove(r.getID());
    }

    /*
     * Register a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage consumer
     */
    public void registerResourceUsageConsumer(ResourceUsageConsumer r) {
        consumerMap.put(r.getID(), r);
    }

    /*
     * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage consumer
     */
    public void unregisterResourceUsageConsumer(ResourceUsageConsumer r) {
        consumerMap.remove(r.getID());
    }

    @Override
    public void close() throws Exception {
        try {
            pTask.close();
            consumer.close();
        } catch (Exception ex1) {
            LOG.error("Error closing producer/consumer for resource-usage topic", ex1);
        }
    }
}
