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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.impl.conf.TopicConsumerConfigurationData;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicConsumerBuilderImplTest {
    private TopicConsumerConfigurationData topicConsumerConfigurationData;
    private TopicConsumerBuilderImpl<String> topicConsumerBuilderImpl;

    @SuppressWarnings("unchecked")
    @BeforeMethod(alwaysRun = true)
    public void setup() {
        ConsumerBuilder<String> consumerBuilder = mock(ConsumerBuilder.class);
        topicConsumerConfigurationData = mock(TopicConsumerConfigurationData.class);
        topicConsumerBuilderImpl = new TopicConsumerBuilderImpl<>(consumerBuilder, topicConsumerConfigurationData);
    }

    @Test
    public void testInvalidPriorityLevel() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> topicConsumerBuilderImpl.priorityLevel(-1));
        verify(topicConsumerConfigurationData, never()).setPriorityLevel(anyInt());
    }

    @Test
    public void testValidPriorityLevel() {
        topicConsumerBuilderImpl.priorityLevel(0);
        verify(topicConsumerConfigurationData).setPriorityLevel(0);
    }
}