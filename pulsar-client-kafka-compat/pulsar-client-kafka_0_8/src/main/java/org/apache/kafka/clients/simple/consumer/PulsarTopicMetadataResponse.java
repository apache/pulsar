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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;

import com.google.common.collect.Lists;

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarTopicMetadataResponse extends TopicMetadataResponse {

    private final List<String> topics;
    private final String hostUrl;
    private final int port;
    private final PulsarAdmin admin;

    public PulsarTopicMetadataResponse(PulsarAdmin admin, String hostUrl, int port, List<String> topics) {
        super(null);
        this.hostUrl = hostUrl;
        this.port = port;
        this.topics = topics;
        this.admin = admin;
    }

    @Override
    public List<TopicMetadata> topicsMetadata() {
        List<TopicMetadata> metadataList = Lists.newArrayList();
        topics.forEach(topic -> {
            try {
                int partitions;
                partitions = admin.topics().getPartitionedTopicMetadata(topic).partitions;
                if (partitions > 0) {
                    for (int partition = 0; partition < partitions; partition++) {
                        String topicName = TopicName.get(topic).getPartition(partition).toString();
                        metadataList.add(new PulsarTopicMetadata(hostUrl, port, topicName));
                    }
                } else {
                    metadataList.add(new PulsarTopicMetadata(hostUrl, port, topic));
                }
            } catch (PulsarAdminException e) {
                log.error("Failed to get partitioned metadata for {}", topic, e);
                throw new RuntimeException("Failed to get partitioned-metadata", e);
            }
        });
        return metadataList;
    }

}