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

package org.apache.pulsar.functions.instance;

import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.functions.proto.Function.ProducerSpec;
import org.apache.pulsar.functions.source.TopicSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class PulsarCluster {
    @Getter
    private PulsarClient client;

    @Getter
    private PulsarAdmin adminClient;

    @Getter
    private final TopicSchema topicSchema;

    @Getter
    private ProducerBuilderImpl<?> producerBuilder;

    @Getter
    private Map<String, Producer<?>> publishProducers;

    @Getter
    private ThreadLocal<Map<String, Producer<?>>> tlPublishProducers;

    public PulsarCluster(PulsarClient client, PulsarAdmin adminClient, ProducerSpec producerSpec) {
        this.client = client;
        this.adminClient = adminClient;
        this.topicSchema = new TopicSchema(client);
        this.producerBuilder = (ProducerBuilderImpl<?>) client.newProducer().blockIfQueueFull(true).enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        boolean useThreadLocalProducers = false;
        if (producerSpec != null) {
            if (producerSpec.getMaxPendingMessages() != 0) {
                this.producerBuilder.maxPendingMessages(producerSpec.getMaxPendingMessages());
            }
            if (producerSpec.getMaxPendingMessagesAcrossPartitions() != 0) {
                this.producerBuilder.maxPendingMessagesAcrossPartitions(producerSpec.getMaxPendingMessagesAcrossPartitions());
            }
            if (producerSpec.getBatchBuilder() != null) {
                if (producerSpec.getBatchBuilder().equals("KEY_BASED")) {
                    this.producerBuilder.batcherBuilder(BatcherBuilder.KEY_BASED);
                } else {
                    this.producerBuilder.batcherBuilder(BatcherBuilder.DEFAULT);
                }
            }
            useThreadLocalProducers = producerSpec.getUseThreadLocalProducers();
        }
        if (useThreadLocalProducers) {
            tlPublishProducers = new ThreadLocal<>();
        } else {
            publishProducers = new HashMap<>();
        }
    }
}
