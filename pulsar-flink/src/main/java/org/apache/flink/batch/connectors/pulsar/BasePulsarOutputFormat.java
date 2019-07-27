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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base Pulsar Output Format to write Flink DataSets into a Pulsar topic.
 */
public abstract class BasePulsarOutputFormat<T> extends RichOutputFormat<T>  {

    private static final Logger LOG = LoggerFactory.getLogger(BasePulsarOutputFormat.class);
    private static final long serialVersionUID = 2304601727522060427L;

    private transient Function<Throwable, MessageId> failureCallback;
    private static volatile Producer<byte[]> producer;

    protected SerializationSchema<T> serializationSchema;

    private ClientConfigurationData clientConf;
    private ProducerConfigurationData producerConf;


    protected BasePulsarOutputFormat(final String serviceUrl, final String topicName,
        final Authentication authentication) {
        Preconditions.checkArgument(StringUtils.isNotBlank(serviceUrl), "serviceUrl cannot be blank.");
        Preconditions.checkArgument(StringUtils.isNotBlank(topicName),  "topicName cannot be blank.");

        clientConf = new ClientConfigurationData();
        producerConf = new ProducerConfigurationData();

        this.clientConf.setServiceUrl(serviceUrl);
        this.clientConf.setAuthentication(authentication);
        this.producerConf.setTopicName(topicName);

        LOG.info("PulsarOutputFormat is being started to write batches to Pulsar topic: {}",
            this.producerConf.getTopicName());
    }

    protected BasePulsarOutputFormat(ClientConfigurationData clientConf, ProducerConfigurationData producerConf) {
        this.clientConf = Preconditions.checkNotNull(clientConf, "client config data should not be null");
        this.producerConf = Preconditions.checkNotNull(producerConf, "producer config data should not be null");

        Preconditions.checkArgument(StringUtils.isNotBlank(clientConf.getServiceUrl()), "serviceUrl cannot be blank.");
        Preconditions.checkArgument(StringUtils.isNotBlank(producerConf.getTopicName()),  "topicName cannot be blank.");

        LOG.info("PulsarOutputFormat is being started to write batches to Pulsar topic: {}",
            this.producerConf.getTopicName());
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.producer = getProducerInstance();

        this.failureCallback = cause -> {
            LOG.error("Error while sending record to Pulsar: " + cause.getMessage(), cause);
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

    private Producer<byte[]> getProducerInstance()
            throws PulsarClientException {
        if (producer == null){
            synchronized (PulsarOutputFormat.class) {
                if (producer == null){
                    producer = Preconditions.checkNotNull(createPulsarProducer(),
                            "Pulsar producer cannot be null.");
                }
            }
        }
        return producer;
    }

    private Producer<byte[]> createPulsarProducer()
            throws PulsarClientException {
        try {
            PulsarClientImpl client = new PulsarClientImpl(clientConf);
            return client.createProducerAsync(producerConf).get();
        } catch (PulsarClientException | InterruptedException | ExecutionException e) {
            LOG.error("Pulsar producer cannot be created.", e);
            throw new PulsarClientException(e);
        }
    }
}
