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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.util.MessageIdUtils;

import com.google.common.collect.Maps;

import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetCommitRequest;

public class PulsarOffsetCommitRequest extends OffsetCommitRequest {
    private final String groupId;
    private final Map<String, MessageId> topicOffsetMap = Maps.newHashMap();

    public PulsarOffsetCommitRequest(String groupId, Map<TopicAndPartition, PulsarOffsetMetadataAndError> requestInfo,
            short versionId, int correlationId, String clientId) {
        super(groupId, Collections.emptyMap(), versionId, correlationId, clientId);
        this.groupId = groupId;
        for (Entry<TopicAndPartition, PulsarOffsetMetadataAndError> topicOffset : requestInfo.entrySet()) {
            String topicName = PulsarKafkaSimpleConsumer.getTopicName(topicOffset.getKey());
            OffsetMetadataAndError offsetMetadata = topicOffset.getValue();
            MessageId msgId = null;
            if (offsetMetadata instanceof PulsarOffsetMetadataAndError) {
                msgId = ((PulsarOffsetMetadataAndError) offsetMetadata).getMessageId();
            }
            msgId = msgId == null ? MessageIdUtils.getMessageId(topicOffset.getValue().offset()) : msgId;
            topicOffsetMap.put(topicName, msgId);
        }
    }

    public String getGroupId() {
        return groupId;
    }

    public Map<String, MessageId> getTopicOffsetMap() {
        return topicOffsetMap;
    }
}