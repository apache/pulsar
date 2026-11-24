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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Checks policy inheritance through the public API and the loaded topic's effective policy. */
@Test(groups = "broker-impl", singleThreaded = true, timeOut = 30000)
public class AckStatePausePolicyTest extends SharedPulsarBaseTest {
    private boolean previousBrokerPolicy;

    @BeforeMethod(alwaysRun = true)
    public void saveConfiguration() {
        previousBrokerPolicy = getConfig().isDispatcherPauseOnAckStatePersistentEnabled();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreConfiguration() {
        getConfig().setDispatcherPauseOnAckStatePersistentEnabled(previousBrokerPolicy);
    }

    @DataProvider(name = "policyScenarios")
    public Object[][] policyScenarios() {
        return new Object[][] {
                {"all policies unset", false, null, null, false},
                {"broker enabled", true, null, null, true},
                {"namespace enabled", false, true, null, true},
                {"namespace disabled", true, false, null, false},
                {"topic enabled", false, null, true, true},
                {"topic disabled", true, true, false, false}
        };
    }

    @DataProvider(name = "policyScopes")
    public Object[][] policyScopes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "policyScenarios")
    public void testPolicyInheritance(String scenario, boolean brokerPolicy, Boolean namespacePolicy,
                                      Boolean topicPolicy, boolean expected) throws Exception {
        getConfig().setDispatcherPauseOnAckStatePersistentEnabled(brokerPolicy);
        if (Boolean.TRUE.equals(namespacePolicy)) {
            admin.namespaces().setDispatcherPauseOnAckStatePersistent(getNamespace());
        } else if (Boolean.FALSE.equals(namespacePolicy)) {
            admin.namespaces().removeDispatcherPauseOnAckStatePersistent(getNamespace());
        }

        for (boolean isGlobal : new boolean[] {false, true}) {
            String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);
            TopicPolicies topicPolicies = admin.topicPolicies(isGlobal);
            setTopicPolicy(topicPolicies, topicName, topicPolicy);

            PersistentTopic topic = getPersistentTopic(topicName);
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(topic.isDispatcherPauseOnAckStatePersistentEnabled())
                            .as("runtime value for %s in %s scope", scenario, scopeName(isGlobal))
                            .isEqualTo(expected));
            assertThat(topicPolicies.getDispatcherPauseOnAckStatePersistent(topicName, false)
                    .get(5, TimeUnit.SECONDS))
                    .as("raw value for %s in %s scope", scenario, scopeName(isGlobal))
                    .isEqualTo(Boolean.TRUE.equals(topicPolicy));
            assertThat(topicPolicies.getDispatcherPauseOnAckStatePersistent(topicName, true)
                    .get(5, TimeUnit.SECONDS))
                    .as("applied value for %s in %s scope", scenario, scopeName(isGlobal))
                    .isEqualTo(expected);
        }
    }

    @Test(dataProvider = "policyScopes")
    public void testExistingTopicPolicyWithUnsetFieldInheritsBroker(boolean isGlobal) throws Exception {
        getConfig().setDispatcherPauseOnAckStatePersistentEnabled(true);
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        TopicPolicies topicPolicies = admin.topicPolicies(isGlobal);
        topicPolicies.setMaxProducersAsync(topicName, 1).get(5, TimeUnit.SECONDS);

        PersistentTopic topic = getPersistentTopic(topicName);
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(topic.isDispatcherPauseOnAckStatePersistentEnabled()).isTrue());
        assertThat(topicPolicies.getDispatcherPauseOnAckStatePersistent(topicName, false)
                .get(5, TimeUnit.SECONDS)).isFalse();
        assertThat(topicPolicies.getDispatcherPauseOnAckStatePersistent(topicName, true)
                .get(5, TimeUnit.SECONDS))
                .isTrue();
    }

    @Test
    public void testLocalAndGlobalPolicyQueriesRemainIndependent() throws Exception {
        getConfig().setDispatcherPauseOnAckStatePersistentEnabled(false);
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);

        TopicPolicies globalPolicies = admin.topicPolicies(true);
        globalPolicies.setDispatcherPauseOnAckStatePersistent(topicName).get(5, TimeUnit.SECONDS);
        TopicPolicies localPolicies = admin.topicPolicies(false);
        localPolicies.setDispatcherPauseOnAckStatePersistent(topicName).get(5, TimeUnit.SECONDS);
        localPolicies.removeDispatcherPauseOnAckStatePersistent(topicName).get(5, TimeUnit.SECONDS);

        PersistentTopic topic = getPersistentTopic(topicName);
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(topic.isDispatcherPauseOnAckStatePersistentEnabled()).isFalse());
        assertPolicy(localPolicies, topicName, false);
        assertPolicy(globalPolicies, topicName, true);
    }

    private void setTopicPolicy(TopicPolicies topicPolicies, String topicName, Boolean value) throws Exception {
        if (Boolean.TRUE.equals(value)) {
            topicPolicies.setDispatcherPauseOnAckStatePersistent(topicName).get(5, TimeUnit.SECONDS);
        } else if (Boolean.FALSE.equals(value)) {
            topicPolicies.setDispatcherPauseOnAckStatePersistent(topicName).get(5, TimeUnit.SECONDS);
            topicPolicies.removeDispatcherPauseOnAckStatePersistent(topicName).get(5, TimeUnit.SECONDS);
        }
    }

    private PersistentTopic getPersistentTopic(String topicName) throws Exception {
        return (PersistentTopic) getTopicIfExists(topicName).get(5, TimeUnit.SECONDS).orElseThrow();
    }

    private void assertPolicy(TopicPolicies topicPolicies, String topicName, boolean expected) throws Exception {
        assertThat(topicPolicies.getDispatcherPauseOnAckStatePersistent(topicName, false)
                .get(5, TimeUnit.SECONDS)).isEqualTo(expected);
        assertThat(topicPolicies.getDispatcherPauseOnAckStatePersistent(topicName, true)
                .get(5, TimeUnit.SECONDS)).isEqualTo(expected);
    }

    private String scopeName(boolean isGlobal) {
        return isGlobal ? "global" : "local";
    }
}
