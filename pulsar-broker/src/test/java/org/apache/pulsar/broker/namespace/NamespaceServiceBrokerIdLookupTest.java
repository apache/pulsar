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
package org.apache.pulsar.broker.namespace;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceServiceBrokerIdLookupTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] namespaceTenants() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "namespaceTenants", timeOut = 60000)
    public void testMultiColonNamespaceUsesNormalBrokerSelection(boolean heartbeatTenant) throws Exception {
        NamespaceName testNamespace = NamespaceName.get(getNamespace());
        String tenant = heartbeatTenant ? NamespaceName.SYSTEM_NAMESPACE.getTenant() : testNamespace.getTenant();
        String namespace = tenant + "/" + testNamespace.getLocalName() + ":bar:123";
        admin.namespaces().createNamespace(namespace);
        try {
            TopicName topic = TopicName.get("persistent://" + namespace + "/topic");
            NamespaceService service = getPulsar().getNamespaceService();
            var bundle = service.getBundleAsync(topic).get(10, TimeUnit.SECONDS);
            assertThat(service.checkOwnershipPresentAsync(bundle).get(10, TimeUnit.SECONDS))
                    .as("a new namespace must exercise broker selection for an unowned bundle").isFalse();

            var lookup = service.getBrokerServiceUrlAsync(topic, LookupOptions.builder().build())
                    .get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(lookup.isBrokerUrl()).isTrue();
            assertThat(lookup.getLookupData().getBrokerId()).isEqualTo(getPulsar().getBrokerId());
            assertThat(lookup.getLookupData().getBrokerUrl()).isEqualTo(getBrokerServiceUrl());

            try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                         .topic(topic.toString()).subscriptionName("sub").subscribe();
                 Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                         .topic(topic.toString()).enableBatching(false).create()) {
                producer.sendAsync("message").get(10, TimeUnit.SECONDS);
                Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.getValue()).isEqualTo("message");
                consumer.acknowledgeAsync(message).get(10, TimeUnit.SECONDS);
            }
        } finally {
            admin.namespaces().deleteNamespace(namespace, true);
        }
    }
}
