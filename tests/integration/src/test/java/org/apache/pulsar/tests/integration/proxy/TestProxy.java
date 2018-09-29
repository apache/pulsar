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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

/**
 * Test cases for proxy.
 */
public class TestProxy extends PulsarTestSuite {

    @Test
    public void testProxy() throws Exception {

        final String tenant = "compaction-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfo(Collections.emptySet(), Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
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
            assertEquals(stats.publishers.size(), 1);
        }
    }

}
