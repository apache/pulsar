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
package org.apache.pulsar.storm;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.storm.PulsarSpout.SpoutConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class PulsarSpoutTest {

    @Test
    public void testAckFailedMessage() throws Exception {

        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = spy(new PulsarSpout(conf, builder));

        Message<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(), new byte[0], Schema.BYTES);
        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        doReturn(future).when(consumer).acknowledgeAsync(msg.getMessageId());
        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        spout.fail(msg);
        spout.ack(msg);
        spout.emitNextAvailableTuple();
        verify(consumer, atLeast(1)).receive(anyInt(), any());
    }

    @Test
    public void testPulsarTuple() throws Exception {
        testPulsarSpout(true);
    }

    @Test
    public void testPulsarSpout() throws Exception {
        testPulsarSpout(false);
    }

    public void testPulsarSpout(boolean pulsarTuple) throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setSharedConsumerEnabled(true);
        AtomicBoolean called = new AtomicBoolean(false);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                called.set(true);
                if ("message to be dropped".equals(new String(msg.getData()))) {
                    return null;
                }
                String val = new String(msg.getData());
                if (val.startsWith("stream:")) {
                    String stream = val.split(":")[1];
                    return new PulsarTuple(stream, val);
                }
                return new Values(val);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

        });

        String msgContent = pulsarTuple ? "stream:pstream" : "test";

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = spy(new PulsarSpout(conf, builder));
        TopologyContext context = mock(TopologyContext.class);
        final String componentId = "test-component-id";
        doReturn(componentId).when(context).getThisComponentId();
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map config = new HashMap<>();
        Field field = SharedPulsarClient.class.getDeclaredField("instances");
        field.setAccessible(true);
        ConcurrentMap<String, SharedPulsarClient> instances = (ConcurrentMap<String, SharedPulsarClient>) field
                .get(SharedPulsarClient.class);

        SharedPulsarClient client = mock(SharedPulsarClient.class);
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(client.getSharedConsumer(any())).thenReturn(consumer);
        instances.put(componentId, client);

        Message<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(), msgContent.getBytes(), Schema.BYTES);
        when(consumer.receive(anyInt(), any())).thenReturn(msg);

        spout.open(config, context, collector);
        spout.emitNextAvailableTuple();

        assertTrue(called.get());
        verify(consumer, atLeast(1)).receive(anyInt(), any());
        ArgumentCaptor<Values> capt = ArgumentCaptor.forClass(Values.class);
        if (pulsarTuple) {
            verify(collector, times(1)).emit(eq("pstream"), capt.capture(), eq(msg));
        } else {
            verify(collector, times(1)).emit(capt.capture(), eq(msg));
        }
        Values vals = capt.getValue();
        assertEquals(msgContent, vals.get(0));
    }

}
