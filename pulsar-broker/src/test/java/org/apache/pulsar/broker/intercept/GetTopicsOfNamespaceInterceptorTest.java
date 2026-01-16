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
package org.apache.pulsar.broker.intercept;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetTopicsOfNamespaceInterceptorTest extends ProducerConsumerBase {
    private final String interceptorName = "get_topics_of_namespace_interceptor";

    private CustomizedTopicListingInterceptor topicListingInterceptor;

    @BeforeMethod
    public void setup() throws Exception {
        isTcpLookup = true;
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setEnableBrokerSideSubscriptionPatternEvaluation(true);
        conf.setEnableBrokerTopicListWatcher(false);

        this.enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        Map<String, BrokerInterceptorWithClassLoader> interceptorMap = new HashMap<>();
        BrokerInterceptor interceptor = new CustomizedTopicListingInterceptor();
        this.topicListingInterceptor = (CustomizedTopicListingInterceptor) interceptor;
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        interceptorMap.put(interceptorName, new BrokerInterceptorWithClassLoader(interceptor, narClassLoader));
        pulsarTestContextBuilder.brokerInterceptor(new BrokerInterceptors(interceptorMap));
    }

    @Test
    public void test() throws Exception {
        String namespace = "public/default";
        String subName = "test-sub";

        Map<String, String> expectedProperties =
            Map.of("env", "prod", "region", "us-west");

        String topicName = "persistent://" + namespace + "/test-topic";
        admin.topics().createPartitionedTopic(topicName, 3);
        String topicName2 = "persistent://" + namespace + "/test-topic2";
        admin.topics().createPartitionedTopic(topicName2, 3);
        String topicName3 = "persistent://" + namespace + "/test-topic3-non-partitioned";

        String topicName4 = "persistent://" + namespace + "/test-topic4";
        admin.topics().createPartitionedTopic(topicName4, 3);

        BrokerInterceptors listener = (BrokerInterceptors) pulsar.getBrokerInterceptor();
        assertNotNull(listener);
        BrokerInterceptorWithClassLoader brokerInterceptor = listener.getInterceptors().get(interceptorName);
        assertNotNull(brokerInterceptor);
        BrokerInterceptor interceptor = brokerInterceptor.getInterceptor();
        assertTrue(interceptor instanceof CustomizedTopicListingInterceptor);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName3).create();
        for (int i = 0; i < 10; i++) {
            producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        topicListingInterceptor.setCustomProperties(TopicName.get(topicName), expectedProperties);
        topicListingInterceptor.setCustomProperties(TopicName.get(topicName2), expectedProperties);
        topicListingInterceptor.setCustomProperties(TopicName.get(topicName3), expectedProperties);

        admin.topics().updateProperties(topicName4, Map.of("env", "test", "region", "us-west"));

        Map<String, String> propsFromClient = new HashMap<>(expectedProperties);

        Set<String> expectedTopics = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            expectedTopics.add(topicName + "-partition-" + i);
        }
        for (int i = 0; i < 3; i++) {
            expectedTopics.add(topicName2 + "-partition-" + i);
        }
        expectedTopics.add(topicName3);

        Awaitility.await().untilAsserted(() -> {
            List<String> list = admin.topics().getList("public/default", TopicDomain.persistent,
                ListTopicsOptions.builder().properties(propsFromClient).build());
            Assert.assertEquals(new HashSet<>(list), expectedTopics);
        });

        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient
            .newConsumer()
            .topicsPattern("persistent://public/default/.*")
            .subscriptionName(subName)
            .properties(propsFromClient)
            .subscribe();

        Set<String> actualTopics = new HashSet<>(consumer.getPartitions());
        Assert.assertEquals(actualTopics, expectedTopics);
    }
}