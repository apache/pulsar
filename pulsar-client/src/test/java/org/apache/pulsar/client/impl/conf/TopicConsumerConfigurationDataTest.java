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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import java.util.regex.Pattern;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TopicConsumerConfigurationDataTest {
    @Test
    public void testOfFactoryMethod() {
        TopicConsumerConfigurationData topicConsumerConfigurationData = TopicConsumerConfigurationData
                .ofTopicName("foo", 1);

        assertThat(topicConsumerConfigurationData.getTopicNameMatcher().matches("foo")).isTrue();
        assertThat(topicConsumerConfigurationData.getPriorityLevel()).isEqualTo(1);
    }

    @Test
    public void testOfDefaultFactoryMethod() {
        ConsumerConfigurationData<Object> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setPriorityLevel(1);
        TopicConsumerConfigurationData topicConsumerConfigurationData = TopicConsumerConfigurationData
                .ofTopicName("foo", consumerConfigurationData);

        assertThat(topicConsumerConfigurationData.getTopicNameMatcher().matches("foo")).isTrue();
        assertThat(topicConsumerConfigurationData.getPriorityLevel()).isEqualTo(1);
    }

    @DataProvider(name = "topicNameMatch")
    public Object[][] topicNameMatch() {
        return new Object[][] {
                new Object[] {"foo", true},
                new Object[] {"bar", false}
        };
    }

    @Test(dataProvider = "topicNameMatch")
    public void testTopicNameMatch(String topicName, boolean expectedMatch) {
        TopicConsumerConfigurationData topicConsumerConfigurationData = TopicConsumerConfigurationData
                .ofTopicsPattern(Pattern.compile("^foo$"), 1);
        assertThat(topicConsumerConfigurationData.getTopicNameMatcher().matches(topicName)).isEqualTo(expectedMatch);
    }

    @Test
    public void testNullTopicsPattern() {
        assertThatNullPointerException()
                .isThrownBy(() -> TopicConsumerConfigurationData.ofTopicsPattern(null, 1));
    }

    @Test
    public void testTopicNameMatchNullTopicName() {
        assertThat(TopicConsumerConfigurationData
                .ofTopicName("foo", 1).getTopicNameMatcher().matches(null)).isFalse();
    }
}
