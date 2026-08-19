/*
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
package org.apache.pulsar.client.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.mockito.stubbing.Answer;
import org.testng.Assert;

/**
 * An implement of {@link PulsarClientTool} for test, which will publish messages iff there is enough messages
 * in the batch.
 */
public class PulsarClientToolForceBatchNum extends PulsarClientTool {
    private final String topic;
    private final int batchNum;

    /**
     *
     * @param properties properties
     * @param topic topic
     * @param batchNum iff there is batchNum messages in the batch, the producer will flush and send.
     */
    public PulsarClientToolForceBatchNum(Properties properties, String topic, int batchNum) {
        super(properties);
        this.topic = topic;
        this.batchNum = batchNum;
        produceCommand = new CmdProduce() {
            @Override
            public void updateConfig(ClientBuilder newBuilder, Authentication authentication, String serviceURL) {
                try {
                    super.updateConfig(mockClientBuilder(newBuilder), authentication, serviceURL);
                } catch (Exception e) {
                    Assert.fail("update config fail " + e.getMessage());
                }
            }
        };
        replaceProducerCommand(produceCommand);
    }

    private ClientBuilder mockClientBuilder(ClientBuilder newBuilder) throws Exception {
        PulsarClientImpl client = (PulsarClientImpl) newBuilder.build();
        // Batch exactly batchNum messages and (practically) never flush on the timer, so that batching is
        // deterministic. CmdProduce leaves batching alone unless it was asked to disable it.
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
            .batchingMaxBytes(Integer.MAX_VALUE)
            .batchingMaxMessages(batchNum)
            .batchingMaxPublishDelay(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            .topic(topic);

        PulsarClientImpl mockClient = spy(client);
        ProducerBuilder<byte[]> mockProducerBuilder = spy(producerBuilder);
        ClientBuilder mockClientBuilder = spy(newBuilder);

        // Create the producer only once CmdProduce asks for it. It configures the builder it was handed
        // first, and options such as --disable-batching have to reach the producer that is created from
        // it: a producer holds the configuration it was created with, so a builder change made after
        // creation no longer affects it.
        doAnswer(invocation -> forceAsyncSend((Producer<byte[]>) invocation.callRealMethod()))
                .when(mockProducerBuilder).create();
        doReturn(mockProducerBuilder).when(mockClient).newProducer(any(Schema.class));
        doReturn(mockClient).when(mockClientBuilder).build();
        return mockClientBuilder;
    }

    /**
     * Wraps the producer so that the synchronous {@code newMessage().send()} the CLI uses is dispatched
     * asynchronously, letting messages accumulate into a batch instead of flushing one by one.
     */
    private static Producer<byte[]> forceAsyncSend(Producer<byte[]> producer) {
        Producer<byte[]> mockProducer = spy(producer);
        doAnswer((Answer<TypedMessageBuilder>) invocation -> {
            TypedMessageBuilder typedMessageBuilder = spy((TypedMessageBuilder) invocation.callRealMethod());
            doAnswer((Answer<MessageId>) invocation1 -> {
                TypedMessageBuilder mock = ((TypedMessageBuilder) invocation1.getMock());
                // using sendAsync() to replace send()
                mock.sendAsync();
                return null;
            }).when(typedMessageBuilder).send();
            return typedMessageBuilder;
        }).when(mockProducer).newMessage();
        return mockProducer;
    }
}
