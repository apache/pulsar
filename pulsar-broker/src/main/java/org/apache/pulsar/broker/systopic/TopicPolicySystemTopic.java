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
package org.apache.pulsar.broker.systopic;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * System topic for topic policy
 */
public class TopicPolicySystemTopic extends SystemTopicBase {

    public TopicPolicySystemTopic(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected Writer createWriter() throws PulsarClientException {
        Producer<PulsarEvent> producer = client.newProducer(Schema.AVRO(PulsarEvent.class))
            .topic(topicName.toString())
            .create();
        return new TopicPolicyWriter(producer);
    }

    @Override
    protected Reader createReaderInternal() throws PulsarClientException {
        org.apache.pulsar.client.api.Reader<PulsarEvent> reader = client.newReader(Schema.AVRO(PulsarEvent.class))
            .topic(topicName.toString())
            .startMessageId(MessageId.earliest)
            .readCompacted(true)
            .create();
        return new TopicPolicyReader(reader, this);
    }

    private static class TopicPolicyWriter implements Writer {

        private Producer<PulsarEvent> producer;

        private TopicPolicyWriter(Producer<PulsarEvent> producer) {
            this.producer = producer;
        }

        @Override
        public MessageId write(PulsarEvent event) throws PulsarClientException {
            return producer.newMessage().key(getEventKey(event)).value(event).send();
        }

        private String getEventKey(PulsarEvent event) {
            return TopicName.get(event.getTopicEvent().getDomain(),
                event.getTopicEvent().getTenant(),
                event.getTopicEvent().getNamespace(),
                event.getTopicEvent().getTopic()).toString();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
        }
    }

    public static class TopicPolicyReader implements Reader {

        private org.apache.pulsar.client.api.Reader<PulsarEvent> reader;
        private final TopicPolicySystemTopic systemTopic;

        private TopicPolicyReader(org.apache.pulsar.client.api.Reader<PulsarEvent> reader,
                                  TopicPolicySystemTopic systemTopic) {
            this.reader = reader;
            this.systemTopic = systemTopic;
        }

        @Override
        public Message<PulsarEvent> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<PulsarEvent>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public void close() throws IOException {
            systemTopic.readers.remove(this);
            this.reader.close();
        }
    }
}
