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
package org.apache.pulsar.client.api;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.client.impl.LookupTopicResult;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class LookupPropertiesTest extends MultiBrokerBaseTest {

    private static final String BROKER_KEY = "lookup.broker.id";
    private static final String CLIENT_KEY = "broker.id";

    @Override
    protected void startBroker() throws Exception {
        addCustomConfigs(conf, 0);
        super.startBroker();
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        return addCustomConfigs(getDefaultConf(), additionalBrokerIndex + 10);
    }

    private static ServiceConfiguration addCustomConfigs(ServiceConfiguration config, int index) {
        config.setDefaultNumberOfNamespaceBundles(16);
        config.setLoadBalancerAutoBundleSplitEnabled(false);
        config.setLoadManagerClassName(BrokerIdAwareLoadManager.class.getName());
        config.setLoadBalancerAverageResourceUsageDifferenceThresholdPercentage(100);
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(1000);
        final var properties = new Properties();
        properties.setProperty(BROKER_KEY, "broker-" + index);
        config.setProperties(properties);
        return config;
    }

    @Test
    public void testLookupProperty() throws Exception {
        admin.namespaces().unload("public/default");
        final var topic = "test-lookup-property";
        admin.topics().createPartitionedTopic(topic, 16);
        @Cleanup final var client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .lookupProperties(
                        Collections.singletonMap(CLIENT_KEY, "broker-10")) // broker-10 refers to additionalBrokers[0]
                .build();
        @Cleanup final var producer = (PartitionedProducerImpl<byte[]>) client.newProducer().topic(topic).create();
        Assert.assertNotNull(producer);
        final var connections = producer.getProducers().stream().map(ProducerImpl::getClientCnx)
                .collect(Collectors.toSet());
        Assert.assertEquals(connections.size(), 1);
        final var port = ((InetSocketAddress) connections.stream().findAny().orElseThrow().ctx().channel()
                .remoteAddress()).getPort();
        Assert.assertEquals(port, additionalBrokers.get(0).getBrokerListenPort().orElseThrow());
    }

    @Test
    public void testConcurrentLookupProperties() throws Exception {
        @Cleanup final var client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .build();
        final var futures = new ArrayList<CompletableFuture<LookupTopicResult>>();
        BrokerIdAwareLoadManager.clientIdList.clear();

        final var clientIdList = IntStream.range(0, 10).mapToObj(i -> "key-" + i).toList();
        for (var clientId : clientIdList) {
            client.getConfiguration().setLookupProperties(Collections.singletonMap(CLIENT_KEY, clientId));
            futures.add(client.getLookup().getBroker(TopicName.get("test-concurrent-lookup-properties")));
            client.getConfiguration().setLookupProperties(Collections.emptyMap());
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(clientIdList, BrokerIdAwareLoadManager.clientIdList);
    }

    public static class BrokerIdAwareLoadManager extends ExtensibleLoadManagerImpl {

        static final List<String> clientIdList = Collections.synchronizedList(new ArrayList<>());

        @Override
        public CompletableFuture<Optional<BrokerLookupData>> assign(Optional<ServiceUnitId> topic,
                                                                    ServiceUnitId serviceUnit, LookupOptions options) {
            getClientId(options).ifPresent(clientIdList::add);
            return super.assign(topic, serviceUnit, options);
        }

        @Override
        public CompletableFuture<Optional<String>> selectAsync(ServiceUnitId bundle, Set<String> excludeBrokerSet,
                                                               LookupOptions options) {
            final var clientId = options.getProperties() == null ? null : options.getProperties().get(CLIENT_KEY);
            if (clientId == null) {
                return super.selectAsync(bundle, excludeBrokerSet, options);
            }
            return getBrokerRegistry().getAvailableBrokerLookupDataAsync().thenCompose(brokerLookupDataMap -> {
                final var optBroker = brokerLookupDataMap.entrySet().stream().filter(entry -> {
                    final var brokerId = entry.getValue().properties().get(BROKER_KEY);
                    return brokerId != null && brokerId.equals(clientId);
                }).findAny();
                return optBroker.map(Map.Entry::getKey).map(Optional::of).map(CompletableFuture::completedFuture)
                        .orElseGet(() -> super.selectAsync(bundle, excludeBrokerSet, options));
            });
        }

        private static Optional<String> getClientId(LookupOptions options) {
            if (options.getProperties() == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(options.getProperties().get(CLIENT_KEY));
        }
    }
}
