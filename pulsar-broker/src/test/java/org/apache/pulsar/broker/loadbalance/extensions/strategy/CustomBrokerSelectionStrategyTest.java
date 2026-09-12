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
package org.apache.pulsar.broker.loadbalance.extensions.strategy;

import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Cleanup;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CustomBrokerSelectionStrategyTest extends MultiBrokerBaseTest {

    @Override
    protected void startBroker() throws Exception {
        addCustomConfigs(conf);
        super.startBroker();
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        return addCustomConfigs(getDefaultConf());
    }

    private static ServiceConfiguration addCustomConfigs(ServiceConfiguration conf) {
        conf.setLoadManagerClassName(CustomExtensibleLoadManager.class.getName());
        conf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        conf.setLoadBalancerAutoBundleSplitEnabled(false);
        conf.setDefaultNumberOfNamespaceBundles(8);
        // Don't consider broker's load so the broker will be selected randomly with the default strategy
        conf.setLoadBalancerAverageResourceUsageDifferenceThresholdPercentage(100);
        return conf;
    }

    @Test
    public void testSingleBrokerSelected() throws Exception {
        final var topic = "test-single-broker-selected";
        getAllAdmins().get(0).topics().createPartitionedTopic(topic, 16);
        @Cleanup final var producer = (PartitionedProducerImpl<byte[]>) getAllClients().get(0).newProducer()
                .topic(topic).create();
        Assert.assertNotNull(producer);
        final var connections = producer.getProducers().stream().map(ProducerImpl::getClientCnx)
                .collect(Collectors.toSet());
        Assert.assertEquals(connections.size(), 1);
        final var port = Integer.parseInt(connections.stream().findFirst().orElseThrow().ctx().channel()
                .remoteAddress().toString().replaceAll(".*:", ""));
        final var expectedPort = Stream.concat(Stream.of(pulsar), additionalBrokers.stream())
                .min(Comparator.comparingInt(o -> o.getListenPortHTTP().orElseThrow()))
                .map(PulsarService::getBrokerListenPort)
                .orElseThrow().orElseThrow();
        Assert.assertEquals(port, expectedPort);
    }

    public static class CustomExtensibleLoadManager extends ExtensibleLoadManagerImpl {

        @Override
        public BrokerSelectionStrategy createBrokerSelectionStrategy() {
            // The smallest HTTP port will always be selected because the host parts are all "localhost"
            return (brokers, __, ___) -> brokers.stream().sorted().findFirst();
        }
    }
}
