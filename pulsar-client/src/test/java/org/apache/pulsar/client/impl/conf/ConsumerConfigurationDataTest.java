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
package org.apache.pulsar.client.impl.conf;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import lombok.Cleanup;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConsumerConfigurationDataTest {
    @DataProvider(name = "topicConf")
    public Object[][] topicConf() {
        return new Object[][] {
                new Object[] {"foo", 2},
                new Object[] {"bar", 1}
        };
    }

    @Test(dataProvider = "topicConf")
    public void testTopicConsumerConfigurationData(String topicName, int expectedPriority) {
        ConsumerConfigurationData<String> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setPriorityLevel(1);

        consumerConfigurationData.getTopicConfigurations()
                .add(TopicConsumerConfigurationData.ofTopicsPattern(Pattern.compile("^foo$"), 2));

        TopicConsumerConfigurationData topicConsumerConfigurationData =
                consumerConfigurationData.getMatchingTopicConfiguration(topicName);

        assertThat(topicConsumerConfigurationData.getPriorityLevel()).isEqualTo(expectedPriority);
    }

    @Test
    public void testCloneForTopicKeepsOnlyThatTopic() {
        ConsumerConfigurationData<String> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("my-sub");
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setTopicNames(new TreeSet<>(Set.of("topic-a", "topic-b", "topic-c")));
        conf.getProperties().put("key", "value");

        ConsumerConfigurationData<String> clone = conf.cloneForTopic("topic-b");

        assertThat(clone.getTopicNames()).containsExactly("topic-b");
        assertThat(clone.getSingleTopic()).isEqualTo("topic-b");
        assertThat(clone.getSubscriptionName()).isEqualTo("my-sub");
        assertThat(clone.getSubscriptionType()).isEqualTo(SubscriptionType.Shared);
        assertThat(clone.getProperties()).containsEntry("key", "value");
        // the clone's topic names and properties are its own
        clone.getTopicNames().add("topic-d");
        clone.getProperties().put("other", "value");
        assertThat(conf.getTopicNames()).containsExactly("topic-a", "topic-b", "topic-c");
        assertThat(conf.getProperties()).doesNotContainKey("other");
        // as with clone()
        assertThat(conf.clone().getTopicNames()).containsExactly("topic-a", "topic-b", "topic-c");
    }

    @Test
    public void testSerializable() throws Exception {
        ConsumerConfigurationData<String> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setPriorityLevel(1);
        consumerConfigurationData.setSubscriptionName("my-sub");
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Shared);
        consumerConfigurationData.setReceiverQueueSize(100);
        consumerConfigurationData.setAckTimeoutMillis(1000);
        consumerConfigurationData.setTopicNames(Collections.singleton("my-topic"));

        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
                .maxRedeliverCount(10)
                .retryLetterTopic("retry-topic")
                .deadLetterTopic("dead-topic")
                .build();
        consumerConfigurationData.setDeadLetterPolicy(deadLetterPolicy);

        @Cleanup
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(consumerConfigurationData);
        byte[] serialized = bos.toByteArray();

        // Deserialize
        @Cleanup
        ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
        @Cleanup
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object object = ois.readObject();

        Assert.assertEquals(object.getClass(), ConsumerConfigurationData.class);
        Assert.assertEquals(object, consumerConfigurationData);

        DeadLetterPolicy deserialisedDeadLetterPolicy = ((ConsumerConfigurationData<?>) object).getDeadLetterPolicy();
        Assert.assertNotNull(deserialisedDeadLetterPolicy);
        Assert.assertEquals(deserialisedDeadLetterPolicy, deadLetterPolicy);
    }
}
