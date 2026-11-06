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
package org.apache.pulsar.io;

import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarApiExamplesJar;
import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.scalable.ScalableTopicConstants;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end tests of Pulsar functions that consume and publish with the V5 client.
 */
@Test(groups = "broker-io")
public class PulsarFunctionV5E2ETest extends AbstractPulsarE2ETest {

    private static final String NAMESPACE = "fn-v5";
    private static final String SUBSCRIPTION = "fn-sub";

    private String topic(String domain, String name) {
        return domain + "://" + tenant + "/" + NAMESPACE + "/" + name;
    }

    private FunctionConfig exclamationFunction(String name, String input, String output) throws Exception {
        admin.namespaces().createNamespace(tenant + "/" + NAMESPACE, Set.of("use"));
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(NAMESPACE);
        functionConfig.setName(name);
        functionConfig.setParallelism(1);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setInputs(List.of(input));
        functionConfig.setOutput(output);
        functionConfig.setSubName(SUBSCRIPTION);
        functionConfig.setSubscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest);
        functionConfig.setCleanupSubscription(true);
        return functionConfig;
    }

    private void createFunction(FunctionConfig functionConfig) throws Exception {
        admin.functions().createFunctionWithUrl(functionConfig, getPulsarApiExamplesJar().toURI().toString());
    }

    private static Set<String> receiveValues(QueueConsumer<String> consumer, int count) throws Exception {
        Set<String> values = new HashSet<>();
        while (values.size() < count) {
            Message<String> message = consumer.receive(Duration.ofSeconds(30));
            assertThat(message).as("received %s of %s messages", values.size(), count).isNotNull();
            values.add(message.value());
            consumer.acknowledge(message.id());
        }
        return values;
    }

    @Test(timeOut = 120000)
    public void testSharedFunctionOnScalableTopics() throws Exception {
        String input = topic("topic", "in");
        String output = topic("topic", "out");
        FunctionConfig functionConfig = exclamationFunction("v5-shared", input, output);
        admin.scalableTopics().createScalableTopic(input, 2);
        admin.scalableTopics().createScalableTopic(output, 2);

        try (PulsarClient client = newV5Client();
             QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                     .topic(output)
                     .subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                     .subscribe();
             Producer<String> producer = client.newProducer(Schema.string()).topic(input).create()) {
            // the topic:// topics select the V5 client without setting clientApi
            createFunction(functionConfig);

            Set<String> expected = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                producer.newMessage().key("key-" + i).value("message-" + i).send();
                expected.add("message-" + i + "!");
            }
            assertThat(receiveValues(consumer, 10)).isEqualTo(expected);

            // every input message was acknowledged
            Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() ->
                    assertThat(admin.scalableTopics().getStats(input).getSubscriptions().get(SUBSCRIPTION)
                            .getMsgBacklog()).isZero());
        }

        // cleanupSubscription removes the subscription from the scalable topic
        admin.functions().deleteFunction(tenant, NAMESPACE, "v5-shared");
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() ->
                assertThat(admin.scalableTopics().getStats(input).getSubscriptions()).doesNotContainKey(SUBSCRIPTION));
    }

    @Test(timeOut = 120000)
    public void testKeyOrderedFunctionOnScalableTopics() throws Exception {
        String input = topic("topic", "keyed-in");
        String output = topic("topic", "keyed-out");
        FunctionConfig functionConfig = exclamationFunction("v5-key-ordered", input, output);
        // Key_Shared maps to a V5 stream subscription, which splits the key ranges across the instances
        functionConfig.setRetainKeyOrdering(true);
        functionConfig.setParallelism(2);
        admin.scalableTopics().createScalableTopic(input, 2);
        admin.scalableTopics().createScalableTopic(output, 2);

        try (PulsarClient client = newV5Client();
             StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string())
                     .topic(output)
                     .subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                     .subscribe();
             Producer<String> producer = client.newProducer(Schema.string()).topic(input).create()) {
            createFunction(functionConfig);

            int keys = 4;
            int perKey = 5;
            for (int i = 0; i < perKey; i++) {
                for (int k = 0; k < keys; k++) {
                    producer.newMessage().key("key-" + k).value(k + ":" + i).send();
                }
            }

            Map<String, List<String>> received = new HashMap<>();
            for (int n = 0; n < keys * perKey; n++) {
                Message<String> message = consumer.receive(Duration.ofSeconds(30));
                assertThat(message).as("received %s of %s messages", n, keys * perKey).isNotNull();
                received.computeIfAbsent(message.key().orElseThrow(), k -> new ArrayList<>()).add(message.value());
                consumer.acknowledgeCumulative(message.id());
            }
            for (int k = 0; k < keys; k++) {
                List<String> expected = new ArrayList<>();
                for (int i = 0; i < perKey; i++) {
                    expected.add(k + ":" + i + "!");
                }
                assertThat(received.get("key-" + k)).as("key-%s", k).containsExactlyElementsOf(expected);
            }
        } finally {
            admin.functions().deleteFunction(tenant, NAMESPACE, "v5-key-ordered");
        }
    }

    @Test(timeOut = 120000)
    public void testClientApiV5OnPersistentTopicsAllowsMigration() throws Exception {
        String input = topic("persistent", "legacy-in");
        String output = topic("persistent", "legacy-out");
        FunctionConfig functionConfig = exclamationFunction("v5-persistent", input, output);
        functionConfig.setClientApi(FunctionConfig.ClientApi.V5);
        admin.topics().createNonPartitionedTopic(input);
        admin.topics().createNonPartitionedTopic(output);

        try (PulsarClient client = newV5Client();
             QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                     .topic(output)
                     .subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                     .subscribe()) {
            createFunction(functionConfig);

            try (Producer<String> producer = client.newProducer(Schema.string()).topic(input).create()) {
                for (int i = 0; i < 5; i++) {
                    producer.newMessage().value("before-" + i).send();
                }
            }
            assertThat(receiveValues(consumer, 5)).hasSize(5).allMatch(value -> value.startsWith("before-"));

            // the function's consumer and producer are V5 clients, so the PIP-475 migration does not refuse
            Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
                SubscriptionStats subscription = admin.topics().getStats(input).getSubscriptions().get(SUBSCRIPTION);
                assertThat(subscription.getConsumers()).isNotEmpty().allSatisfy(consumerStats ->
                        assertThat(consumerStats.getMetadata()).containsEntry(
                                ScalableTopicConstants.V5_MANAGED_METADATA_KEY,
                                ScalableTopicConstants.V5_MANAGED_METADATA_VALUE));
            });
            admin.scalableTopics().migrateToScalable(input, false);
        } finally {
            admin.functions().deleteFunction(tenant, NAMESPACE, "v5-persistent");
        }
    }
}
