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
package org.apache.pulsar.netty.common;

import com.google.common.base.Preconditions;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarUtils.class);

    private static volatile Producer<byte[]> producer;

    public static Producer<byte[]> getProducerInstance(String serviceUrl, String topicName) throws PulsarClientException {
        if(producer == null){
            synchronized (PulsarUtils.class) {
                if(producer == null){
                    producer = Preconditions.checkNotNull(createPulsarProducer(serviceUrl, topicName),
                            "Pulsar producer cannot be null.");
                }
            }
        }
        return producer;
    }

    private static Producer<byte[]> createPulsarProducer(String serviceUrl, String topicName) throws PulsarClientException {
        try {
            PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            return client.newProducer().topic(topicName).create();
        } catch (PulsarClientException e) {
            LOG.error("Pulsar producer cannot be created.", e);
            throw e;
        }
    }
}
