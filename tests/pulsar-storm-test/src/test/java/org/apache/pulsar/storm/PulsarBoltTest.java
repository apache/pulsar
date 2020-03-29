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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class PulsarBoltTest extends ProducerConsumerBase {

    private static final int NO_OF_RETRIES = 10;

    public String serviceUrl;
    public final String topic = "persistent://my-property/my-ns/my-topic1";
    public final String subscriptionName = "my-subscriber-name";

    protected PulsarBoltConfiguration pulsarBoltConf;
    protected PulsarBolt bolt;
    protected MockOutputCollector mockCollector;
    protected Consumer consumer;

    @Override
    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        super.beforeMethod(m);
        setup();
    }

    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        serviceUrl = pulsar.getWebServiceAddress();

        pulsarBoltConf = new PulsarBoltConfiguration();
        pulsarBoltConf.setServiceUrl(serviceUrl);
        pulsarBoltConf.setTopic(topic);
        pulsarBoltConf.setTupleToMessageMapper(tupleToMessageMapper);
        pulsarBoltConf.setMetricsTimeIntervalInSecs(60);
        bolt = new PulsarBolt(pulsarBoltConf, PulsarClient.builder());
        mockCollector = new MockOutputCollector();
        OutputCollector collector = new OutputCollector(mockCollector);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-bolt-" + methodName);
        when(context.getThisTaskId()).thenReturn(0);
        bolt.prepare(Maps.newHashMap(), context, collector);
        consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(subscriptionName).subscribe();
    }

    @AfterMethod
    public void cleanup() throws Exception {
        bolt.close();
        consumer.close();
        super.internalCleanup();
    }

    @SuppressWarnings("serial")
    static TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

        @Override
        public TypedMessageBuilder<byte[]> toMessage(TypedMessageBuilder<byte[]> msgBuilder, Tuple tuple) {
            if ("message to be dropped".equals(new String(tuple.getBinary(0)))) {
                return null;
            }
            if ("throw exception".equals(new String(tuple.getBinary(0)))) {
                throw new RuntimeException();
            }
            return msgBuilder.value(tuple.getBinary(0));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    };

    private Tuple getMockTuple(String msgContent) {
        Tuple mockTuple = mock(Tuple.class);
        when(mockTuple.getBinary(0)).thenReturn(msgContent.getBytes());
        when(mockTuple.getSourceComponent()).thenReturn("");
        when(mockTuple.getSourceStreamId()).thenReturn("");
        return mockTuple;
    }

    @Test
    public void testBasic() throws Exception {
        String msgContent = "hello world";
        Tuple tuple = getMockTuple(msgContent);
        bolt.execute(tuple);
        for (int i = 0; i < NO_OF_RETRIES; i++) {
            Thread.sleep(1000);
            if (mockCollector.acked()) {
                break;
            }
        }
        Assert.assertTrue(mockCollector.acked());
        Assert.assertFalse(mockCollector.failed());
        Assert.assertNull(mockCollector.getLastError());
        Assert.assertEquals(tuple, mockCollector.getAckedTuple());
        Message msg = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledge(msg);
        Assert.assertEquals(msgContent, new String(msg.getData()));
    }

    @Test
    public void testExecuteFailure() throws Exception {
        String msgContent = "throw exception";
        Tuple tuple = getMockTuple(msgContent);
        bolt.execute(tuple);
        Assert.assertFalse(mockCollector.acked());
        Assert.assertTrue(mockCollector.failed());
        Assert.assertNotNull(mockCollector.getLastError());
    }

    @Test
    public void testNoMessageSend() throws Exception {
        String msgContent = "message to be dropped";
        Tuple tuple = getMockTuple(msgContent);
        bolt.execute(tuple);
        Assert.assertTrue(mockCollector.acked());
        Message msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg);
    }

    @Test
    public void testMetrics() throws Exception {
        bolt.resetMetrics();
        String msgContent = "hello world";
        Tuple tuple = getMockTuple(msgContent);
        for (int i = 0; i < 10; i++) {
            bolt.execute(tuple);
        }
        for (int i = 0; i < NO_OF_RETRIES; i++) {
            Thread.sleep(1000);
            if (mockCollector.getNumTuplesAcked() == 10) {
                break;
            }
        }
        @SuppressWarnings("rawtypes")
        Map metrics = (Map) bolt.getValueAndReset();
        Assert.assertEquals(((Long) metrics.get(PulsarBolt.NO_OF_MESSAGES_SENT)).longValue(), 10);
        Assert.assertEquals(((Double) metrics.get(PulsarBolt.PRODUCER_RATE)).doubleValue(),
                10.0 / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        Assert.assertEquals(((Double) metrics.get(PulsarBolt.PRODUCER_THROUGHPUT_BYTES)).doubleValue(),
                ((double) msgContent.getBytes().length * 10) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        metrics = bolt.getMetrics();
        Assert.assertEquals(((Long) metrics.get(PulsarBolt.NO_OF_MESSAGES_SENT)).longValue(), 0);
        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }
    }

    @Test
    public void testSharedProducer() throws Exception {
        TopicStats topicStats = admin.topics().getStats(topic);
        Assert.assertEquals(topicStats.publishers.size(), 1);
        PulsarBolt otherBolt = new PulsarBolt(pulsarBoltConf, PulsarClient.builder());
        MockOutputCollector otherMockCollector = new MockOutputCollector();
        OutputCollector collector = new OutputCollector(otherMockCollector);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-bolt-" + methodName);
        when(context.getThisTaskId()).thenReturn(1);
        otherBolt.prepare(Maps.newHashMap(), context, collector);

        topicStats = admin.topics().getStats(topic);
        Assert.assertEquals(topicStats.publishers.size(), 1);

        otherBolt.close();

        topicStats = admin.topics().getStats(topic);
        Assert.assertEquals(topicStats.publishers.size(), 1);
    }

    @Test
    public void testSerializability() throws Exception {
        // test serializability with no auth
        PulsarBolt boltWithNoAuth = new PulsarBolt(pulsarBoltConf, PulsarClient.builder());
        TestUtil.testSerializability(boltWithNoAuth);
    }

    @Test
    public void testFailedProducer() {
        PulsarBoltConfiguration pulsarBoltConf = new PulsarBoltConfiguration();
        pulsarBoltConf.setServiceUrl(serviceUrl);
        pulsarBoltConf.setTopic("persistent://invalid");
        pulsarBoltConf.setTupleToMessageMapper(tupleToMessageMapper);
        pulsarBoltConf.setMetricsTimeIntervalInSecs(60);
        PulsarBolt bolt = new PulsarBolt(pulsarBoltConf, PulsarClient.builder());
        MockOutputCollector mockCollector = new MockOutputCollector();
        OutputCollector collector = new OutputCollector(mockCollector);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("new" + methodName);
        when(context.getThisTaskId()).thenReturn(0);
        try {
            bolt.prepare(Maps.newHashMap(), context, collector);
            fail("should have failed as producer creation failed");
        } catch (IllegalStateException ie) {
            // Ok.
        }
    }
}
