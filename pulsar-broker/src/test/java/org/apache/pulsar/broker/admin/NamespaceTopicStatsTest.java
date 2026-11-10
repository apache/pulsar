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
package org.apache.pulsar.broker.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class NamespaceTopicStatsTest extends MockedPulsarServiceBaseTest {

    private static final String NAMESPACE = "public/default";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetTopicsStatsReturnsStatsForLoadedTopics() throws Exception {
        String topic1 = "persistent://" + NAMESPACE + "/namespace-stats-1";
        String topic2 = "persistent://" + NAMESPACE + "/namespace-stats-2";
        admin.topics().createNonPartitionedTopic(topic1);
        admin.topics().createNonPartitionedTopic(topic2);
        // A connected producer makes sure the topics are loaded on the broker.
        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topic1).create();
        @Cleanup
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topic2).create();
        producer1.send("hello".getBytes());

        Map<String, TopicStats> stats = admin.namespaces().getTopicsStats(NAMESPACE);

        assertThat(stats).containsKeys(topic1, topic2);
        assertThat(stats.get(topic1).getMsgInCounter()).isGreaterThanOrEqualTo(1);
        assertThat(stats.get(topic1).getPublishers().size()).isGreaterThan(0);
    }

    @Test
    public void testGetTopicsStatsOnMissingNamespace() {
        assertThatThrownBy(() -> admin.namespaces().getTopicsStats("public/nonexistent-namespace"))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
    }
}
