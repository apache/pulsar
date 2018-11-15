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
package org.apache.flink.batch.connectors.pulsar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

/**
 * Flink Batch Sink to write DataSets into a Pulsar topic.
 */
public class PulsarOutputFormat<T> extends RichOutputFormat<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarOutputFormat.class);

    private static String serviceUrl;
    private static String topicName;
    private SerializationSchema<T> serializationSchema;

    private transient Function<Throwable, MessageId> failureCallback;

    private static volatile Producer<byte[]> producer;

    public PulsarOutputFormat(String serviceUrl, String topicName, SerializationSchema<T> serializationSchema) {
        Preconditions.checkNotNull(serviceUrl, "serviceUrl must not be null.");
        Preconditions.checkArgument(StringUtils.isNotBlank(topicName),  "topicName must not be blank.");
        Preconditions.checkNotNull(serializationSchema,  "serializationSchema must not be null.");

        this.serviceUrl = serviceUrl;
        this.topicName = topicName;
        this.serializationSchema = serializationSchema;

        LOG.info("PulsarOutputFormat is being started to write batches to Pulsar topic {}", this.topicName);
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.producer = getProducerInstance();

        this.failureCallback = cause -> {
            LOG.error("Error while sending record to Pulsar : " + cause.getMessage(), cause);
            return null;
        };
    }

    @Override
    public void writeRecord(T t) throws IOException {
        byte[] data = this.serializationSchema.serialize(t);
        this.producer.sendAsync(data)
                .exceptionally(this.failureCallback);
    }

    @Override
    public void close() throws IOException {

    }

    private static Producer<byte[]> getProducerInstance() throws PulsarClientException {
        if(producer == null){
            synchronized (PulsarOutputFormat.class) {
                if(producer == null){
                    producer = Preconditions.checkNotNull(createPulsarProducer(),
                                                                "Pulsar producer must not be null.");
                }
            }
        }
        return producer;
    }

    private static Producer<byte[]> createPulsarProducer() throws PulsarClientException {
        try {
            PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            return client.newProducer().topic(topicName).create();
        } catch (PulsarClientException e) {
            LOG.error("Pulsar producer can not be created.", e);
            throw e;
        }
    }
}
