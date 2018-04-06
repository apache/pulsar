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
package org.apache.pulsar.functions.instance.producers;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.ProducerConfiguration.HashingScheme;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.instance.FunctionResultRouter;

abstract class AbstractOneOuputTopicProducers implements Producers {

    protected final PulsarClient client;
    protected final String outputTopic;
    protected final ProducerConfiguration conf;

    AbstractOneOuputTopicProducers(PulsarClient client,
                                   String outputTopic)
            throws PulsarClientException {
        this.client = client;
        this.outputTopic = outputTopic;
        this.conf = newProducerConfiguration();
    }

    protected ProducerConfiguration newProducerConfiguration() {
        ProducerConfiguration conf = new ProducerConfiguration();
        conf.setBlockIfQueueFull(true);
        conf.setBatchingEnabled(true);
        conf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        conf.setMaxPendingMessages(1000000);
        conf.setCompressionType(CompressionType.LZ4);
        conf.setHashingScheme(HashingScheme.Murmur3_32Hash);
        conf.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        // use function result router to deal with different processing guarantees.
        conf.setMessageRouter(FunctionResultRouter.of());
        return conf;
    }

    protected Producer createProducer(String topic)
            throws PulsarClientException {
        return client.createProducer(topic, newProducerConfiguration());
    }

    protected Producer createProducer(String topic, String producerName)
            throws PulsarClientException {
        ProducerConfiguration newConf = newProducerConfiguration();
        newConf.setProducerName(producerName);

        return client.createProducer(topic, newConf);
    }

}
