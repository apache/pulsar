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
package org.apache.pulsar.storm.example;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.storm.MessageToValuesMapper;
import org.apache.pulsar.storm.PulsarBolt;
import org.apache.pulsar.storm.PulsarBoltConfiguration;
import org.apache.pulsar.storm.PulsarSpout;
import org.apache.pulsar.storm.PulsarSpoutConfiguration;
import org.apache.pulsar.storm.TupleToMessageMapper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormExample {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSpout.class);
    private static final String serviceUrl = "http://broker-pdev.messaging.corp.usw.example.com:8080";

    @SuppressWarnings("serial")
    static MessageToValuesMapper messageToValuesMapper = new MessageToValuesMapper() {

        @Override
        public Values toValues(Message msg) {
            return new Values(new String(msg.getData()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declare the output fields
            declarer.declare(new Fields("string"));
        }
    };

    @SuppressWarnings("serial")
    static TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

        @Override
        public TypedMessageBuilder<byte[]> toMessage(TypedMessageBuilder<byte[]> msgBuilder, Tuple tuple) {
            String receivedMessage = tuple.getString(0);
            // message processing
            String processedMsg = receivedMessage + "-processed";
            return msgBuilder.value(processedMsg.getBytes());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declare the output fields
        }
    };

    public static void main(String[] args) throws Exception {
        // String authPluginClassName = "org.apache.pulsar.client.impl.auth.MyAuthentication";
        // String authParams = "key1:val1,key2:val2";
        // clientConf.setAuthentication(authPluginClassName, authParams);

        String topic1 = "persistent://my-property/use/my-ns/my-topic1";
        String topic2 = "persistent://my-property/use/my-ns/my-topic2";
        String subscriptionName1 = "my-subscriber-name1";
        String subscriptionName2 = "my-subscriber-name2";

        // create spout
        PulsarSpoutConfiguration spoutConf = new PulsarSpoutConfiguration();
        spoutConf.setServiceUrl(serviceUrl);
        spoutConf.setTopic(topic1);
        spoutConf.setSubscriptionName(subscriptionName1);
        spoutConf.setMessageToValuesMapper(messageToValuesMapper);
        PulsarSpout spout = new PulsarSpout(spoutConf, PulsarClient.builder());

        // create bolt
        PulsarBoltConfiguration boltConf = new PulsarBoltConfiguration();
        boltConf.setServiceUrl(serviceUrl);
        boltConf.setTopic(topic2);
        boltConf.setTupleToMessageMapper(tupleToMessageMapper);
        PulsarBolt bolt = new PulsarBolt(boltConf, PulsarClient.builder());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", spout);
        builder.setBolt("testBolt", bolt).shuffleGrouping("testSpout");

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);
        conf.registerMetricsConsumer(PulsarMetricsConsumer.class);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
        // create a consumer on topic2 to receive messages from the bolt when the processing is done
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic2).subscriptionName(subscriptionName2).subscribe();
        // create a producer on topic1 to send messages that will be received by the spout
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic1).create();

        for (int i = 0; i < 10; i++) {
            String msg = "msg-" + i;
            producer.send(msg.getBytes());
            LOG.info("Message {} sent", msg);
        }
        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            LOG.info("Message {} received", new String(msg.getData()));
        }
        cluster.killTopology("test");
        cluster.shutdown();

    }

    class PulsarMetricsConsumer implements IMetricsConsumer {

        @Override
        public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                IErrorReporter errorReporter) {
        }

        @Override
        public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
            // The collection will contain metrics for all the spouts/bolts that register the metrics in the topology.
            // The name for the Pulsar Spout is "PulsarSpoutMetrics-{componentId}-{taskIndex}" and for the Pulsar Bolt
            // is
            // "PulsarBoltMetrics-{componentId}-{taskIndex}".
        }

        @Override
        public void cleanup() {
        }

    }
}
