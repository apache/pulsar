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
package org.apache.pulsar.tests.integration.proxy;

import static org.testng.Assert.assertEquals;
import java.util.Collections;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Test cases for proxy.
 */
public class TestProxy extends PulsarTestSuite {
    private static final Logger log = LoggerFactory.getLogger(TestProxy.class);

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        return super.beforeSetupCluster(clusterName, specBuilder);
    }

    private void testProxy(String serviceUrl, String httpServiceUrl) throws Exception {
        final String tenant = "proxy-test-" + randomName(10);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl(httpServiceUrl)
            .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.emptySet(), Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build();

        client.newConsumer()
                .topic(topic)
                .subscriptionName("sub1")
                .subscribe()
                .close();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        producer.send("content-0");
        producer.send("content-1");

        for (int i = 0; i < 10; i++) {
            // Ensure we can get the stats for the topic irrespective of which broker the proxy decides to connect to
            TopicStats stats = admin.topics().getStats(topic);
            assertEquals(stats.getPublishers().size(), 1);
        }
    }

    @Test
    public void testProxyWithServiceDiscovery() throws Exception {
        testProxy(pulsarCluster.getPlainTextServiceUrl(), pulsarCluster.getHttpServiceUrl());
    }

    @Test
    public void testProxyWithNoServiceDiscoveryProxyConnectsViaURL() throws Exception {
        testProxy(pulsarCluster.getProxy().getPlainTextServiceUrl(), pulsarCluster.getProxy().getHttpServiceUrl());
    }

    @Test
    public void testProxyRequestBodyRedirect() throws Exception {
        // See GH issue #5360, this ensures that we properly get a request with a body to be processed
        final String tenant = "proxy-test-" + randomName(10);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getProxy().getHttpServiceUrl())
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.emptySet(), Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));

        for (int i = 0; i < 10; i++) {
            // Ensure we the command works even if re-directs happen with a request body
            admin.topics().createSubscription(topic, "test-" + i, MessageId.earliest);
        }
    }
}
