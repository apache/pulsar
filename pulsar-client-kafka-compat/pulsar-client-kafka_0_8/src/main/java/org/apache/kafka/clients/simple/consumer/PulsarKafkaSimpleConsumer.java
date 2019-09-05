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
package org.apache.kafka.clients.simple.consumer;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.PulsarClientKafkaConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats.CursorStats;

import com.google.common.collect.Maps;

import kafka.api.FetchRequest;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Note: <br/>
 * - SimpleConsumer doesn't work well with pulsar-batch messages because client-app uses raw-offset to commit offset for
 * a given group-id. <br/>
 * - In order to work with partitioned-topic and batch messages: use custom api
 * {@link #PulsarMsgAndOffset::getFullOffset()} to fetch offset with {@link MessageId} and commit offset with the same
 * message-Id {@link PulsarOffsetMetadataAndError::PulsarOffsetMetadataAndError(messageId..)}.
 *
 */
@Slf4j
public class PulsarKafkaSimpleConsumer extends SimpleConsumer {

    private final String host;
    private final int port;
    private final String clientId;
    private final PulsarClient client;
    private final PulsarAdmin admin;
    private final Map<TopicGroup, Consumer<byte[]>> topicConsumerMap;
    private final SubscriptionType subscriptionType;
    public static final String SUBSCRIPTION_TYPE = "pulsar.subscription.type";
    public static final String HTTP_SERVICE_URL = "pulsar.http.service.url";

    public PulsarKafkaSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
        this(host, port, soTimeout, bufferSize, clientId, new Properties());
    }

    /**
     * 
     * @param host
     *            pulsar-broker service url
     * @param port
     *            n/a
     * @param soTimeout
     *            n/a
     * @param bufferSize
     *            n/a
     * @param clientId
     *            client-id
     * @param properties
     *            properties to retrieve authentication params by {@link PulsarClientKafkaConfig}
     */
    public PulsarKafkaSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId,
            Properties properties) {
        super(host, port, soTimeout, bufferSize, clientId);
        this.host = host;
        this.port = port;
        this.clientId = clientId;
        try {
            client = PulsarClientKafkaConfig.getClientBuilder(properties).serviceUrl(host).build();
        } catch (PulsarClientException e) {
            log.warn("Failed to create pulsar client for {} and properties {}", host, properties);
            throw new RuntimeException("Failed to create pulsar client " + host, e);
        }
        try {
            String url = properties.getProperty(HTTP_SERVICE_URL, host);
            admin = PulsarClientKafkaConfig.getAdminBuilder(url, properties).build();
        } catch (PulsarClientException e) {
            log.warn("Failed to create pulsar admin for {} and properties {}", host, properties);
            throw new RuntimeException("Failed to create pulsar admin " + host, e);
        }
        this.topicConsumerMap = new ConcurrentHashMap<>(8, 0.75f, 1);
        this.subscriptionType = getSubscriptionType(properties);
    }

    @Override
    public PulsarFetchResponse fetch(FetchRequest request) {
        try {
            Map<String, Reader<byte[]>> topicReaderMap = createTopicReaders(request);
            return new PulsarFetchResponse(topicReaderMap, false);
        } catch (Exception e) {
            log.warn("Failed to process fetch request{}, {}", request, e.getMessage());
            return new PulsarFetchResponse(null, true);
        }
    }

    private Map<String, Reader<byte[]>> createTopicReaders(FetchRequest request) {
        Map<String, Reader<byte[]>> topicReaderMap = Maps.newHashMap();
        scala.collection.immutable.Map<String, scala.collection.immutable.Map<TopicAndPartition, PartitionFetchInfo>> reqInfo = request
                .requestInfoGroupedByTopic();
        Map<String, scala.collection.immutable.Map<TopicAndPartition, PartitionFetchInfo>> topicPartitionMap = scala.collection.JavaConverters
                .mapAsJavaMapConverter(reqInfo).asJava();
        for (Entry<String, scala.collection.immutable.Map<TopicAndPartition, PartitionFetchInfo>> topicPartition : topicPartitionMap
                .entrySet()) {
            final String topicName = topicPartition.getKey();
            Map<TopicAndPartition, PartitionFetchInfo> topicOffsetMap = scala.collection.JavaConverters
                    .mapAsJavaMapConverter(topicPartition.getValue()).asJava();
            if (topicOffsetMap != null && !topicOffsetMap.isEmpty()) {
                // pulsar-kafka adapter doesn't deal with partition so, assuming only 1 topic-metadata per topic name
                Entry<TopicAndPartition, PartitionFetchInfo> topicOffset = topicOffsetMap.entrySet().iterator().next();
                long offset = topicOffset.getValue().offset();
                String topic = getTopicName(topicOffset.getKey());
                MessageId msgId = getMessageId(offset);
                try {
                    Reader<byte[]> reader = client.newReader().readerName(clientId).topic(topic).startMessageId(msgId)
                            .create();
                    log.info("Successfully created reader for {} at msg-id {}", topic, msgId);
                    topicReaderMap.put(topicName, reader);
                } catch (PulsarClientException e) {
                    log.warn("Failed to create reader for topic {}", topic, e);
                    throw new RuntimeException("Failed to create reader for " + topic, e);
                }
            }
        }
        return topicReaderMap;
    }

    private MessageId getMessageId(long offset) {
        if (kafka.api.OffsetRequest.EarliestTime() == offset) {
            return MessageId.earliest;
        } else if (kafka.api.OffsetRequest.LatestTime() == offset) {
            return MessageId.latest;
        } else {
            return MessageIdUtils.getMessageId(offset);
        }
    }

    @Override
    public PulsarTopicMetadataResponse send(TopicMetadataRequest request) {
        List<String> topics = request.topics();
        PulsarTopicMetadataResponse response = new PulsarTopicMetadataResponse(admin, host, port, topics);
        return response;
    }

    // It's @Overriden method of: OffsetResponse getOffsetsBefore(OffsetRequest or)
    public PulsarOffsetResponse getOffsetsBefore(PulsarOffsetRequest or) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> request = or.getRequestInfo();
        Map<TopicAndPartition, Long> offsetResoponse = Maps.newHashMap();
        for (Entry<TopicAndPartition, PartitionOffsetRequestInfo> topicPartitionRequest : request.entrySet()) {
            TopicAndPartition topic = topicPartitionRequest.getKey();
            long time = topicPartitionRequest.getValue().time();
            if (time != kafka.api.OffsetRequest.EarliestTime() && time != kafka.api.OffsetRequest.LatestTime()) {
                throw new IllegalArgumentException("Time has to be from EarliestTime or LatestTime");
            }
            offsetResoponse.put(topic, time);
        }
        return new PulsarOffsetResponse(offsetResoponse);
    }

    /**
     * <pre>
     * Overriden method: OffsetCommitResponse commitOffsets(OffsetCommitRequest request)
     * 
     * Note:
     * created PulsarOffsetCommitResponse as OffsetCommitRequest doesn't provide getters
     * 
     * </pre>
     */
    public OffsetCommitResponse commitOffsets(PulsarOffsetCommitRequest request) {

        PulsarOffsetCommitResponse response = new PulsarOffsetCommitResponse(null);
        for (Entry<String, MessageId> topicOffset : request.getTopicOffsetMap().entrySet()) {
            final String topic = topicOffset.getKey();
            final String groupId = request.getGroupId();
            try {
                Consumer<byte[]> consumer = getConsumer(topic, groupId);
                consumer.acknowledgeCumulative(topicOffset.getValue());
            } catch (Exception e) {
                log.warn("Failed to ack message for topic {}-{}", topic, topicOffset.getValue(), e);
                response.hasError = true;
                TopicAndPartition topicPartition = new TopicAndPartition(topic, 0);
                response.errors.computeIfAbsent(topicPartition, tp -> ErrorMapping.UnknownCode());
            }
        }

        return response;
    }

    /**
     * <pre>
     * Overriden method: OffsetFetchResponse fetchOffsets(OffsetFetchRequest request)
     * 
     * Note:
     * created PulsarOffsetFetchRequest as OffsetFetchRequest doesn't have getters for any field
     * and PulsarOffsetFetchResponse created as base-class doesn't have setters to set state
     * &#64;param request
     * &#64;return
     * 
     * </pre>
     */
    public PulsarOffsetFetchResponse fetchOffsets(PulsarOffsetFetchRequest request) {
        final String groupId = request.groupId;
        Map<TopicAndPartition, OffsetMetadataAndError> responseMap = Maps.newHashMap();
        PulsarOffsetFetchResponse response = new PulsarOffsetFetchResponse(responseMap);
        for (TopicAndPartition topicMetadata : request.requestInfo) {
            final String topicName = getTopicName(topicMetadata);
            try {
                PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
                CursorStats cursor = stats.cursors != null ? stats.cursors.get(groupId) : null;
                if (cursor != null) {
                    String readPosition = cursor.readPosition;
                    MessageId msgId = null;
                    if (readPosition != null && readPosition.contains(":")) {
                        try {
                            String[] position = readPosition.split(":");
                            msgId = new MessageIdImpl(Long.parseLong(position[0]), Long.parseLong(position[1]), -1);
                        } catch (Exception e) {
                            log.warn("Invalid read-position {} for {}-{}", readPosition, topicName, groupId);
                        }
                    }
                    msgId = msgId == null ? MessageId.earliest : msgId;
                    OffsetMetadataAndError oE = new OffsetMetadataAndError(MessageIdUtils.getOffset(msgId), null,
                            ErrorMapping.NoError());
                    responseMap.put(topicMetadata, oE);
                }
            } catch (Exception e) {
                OffsetMetadataAndError oE = new OffsetMetadataAndError(0, null, ErrorMapping.UnknownCode());
                responseMap.put(topicMetadata, oE);
            }
        }
        return response;
    }

    public static String getTopicName(TopicAndPartition topicMetadata) {
        return topicMetadata.partition() > -1
                ? TopicName.get(topicMetadata.topic()).getPartition(topicMetadata.partition()).toString()
                : topicMetadata.topic();
    }

    @Override
    public void close() {

        if (topicConsumerMap != null) {
            topicConsumerMap.forEach((topic, consumer) -> {
                try {
                    consumer.close();
                } catch (PulsarClientException e) {
                    log.warn("Failed to close consumer for topic {}", topic, e);
                }
            });
            topicConsumerMap.clear();
        }

        if (client != null) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar-client ", e);
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (Exception e) {
                log.warn("Failed to close pulsar-admin ", e);
            }
        }
    }

    private Consumer<byte[]> getConsumer(String topic, String groupId) {
        TopicGroup topicGroup = new TopicGroup(topic, groupId);

        return topicConsumerMap.computeIfAbsent(topicGroup, (topicName) -> {
            try {
                return client.newConsumer().topic(topic).subscriptionName(groupId).subscriptionType(subscriptionType)
                        .subscribe();
            } catch (PulsarClientException e) {
                log.error("Failed to create consumer for topic {}", topic, e);
                throw new RuntimeException("Failed to create consumer for topic " + topic, e);
            }
        });
    }

    public static class TopicGroup {
        protected String topic;
        protected String grouoId;

        public TopicGroup(String topic, String grouoId) {
            super();
            this.topic = topic;
            this.grouoId = grouoId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, grouoId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TopicGroup) {
                TopicGroup t = (TopicGroup) obj;
                return Objects.equals(topic, t.topic) && Objects.equals(grouoId, t.grouoId);
            }
            return false;
        }

    }

    private SubscriptionType getSubscriptionType(Properties properties) {
        String subType = properties != null && properties.contains(SUBSCRIPTION_TYPE)
                ? properties.getProperty(SUBSCRIPTION_TYPE)
                : SubscriptionType.Failover.toString();
        try {
            return SubscriptionType.valueOf(subType);
        } catch (IllegalArgumentException ie) {
            return SubscriptionType.Failover;
        }
    }
}
