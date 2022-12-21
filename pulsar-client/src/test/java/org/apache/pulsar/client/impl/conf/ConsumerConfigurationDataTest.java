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
import java.util.regex.Pattern;
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
}
