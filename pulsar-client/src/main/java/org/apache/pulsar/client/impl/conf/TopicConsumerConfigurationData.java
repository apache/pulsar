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

import java.io.Serializable;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicConsumerConfigurationData implements Serializable {
    private static final long serialVersionUID = 1L;

    private TopicNameMatcher topicNameMatcher;
    private int priorityLevel;

    public static TopicConsumerConfigurationData ofTopicsPattern(@NonNull Pattern topicsPattern, int priorityLevel) {
        return of(new TopicNameMatcher.TopicsPattern(topicsPattern), priorityLevel);
    }

    public static TopicConsumerConfigurationData ofTopicsPattern(@NonNull Pattern topicsPattern,
                                                                 ConsumerConfigurationData<?> conf) {
        return ofTopicsPattern(topicsPattern, conf.getPriorityLevel());
    }

    public static TopicConsumerConfigurationData ofTopicName(@NonNull String topicName, int priorityLevel) {
        return of(new TopicNameMatcher.TopicName(topicName), priorityLevel);
    }

    public static TopicConsumerConfigurationData ofTopicName(@NonNull String topicName,
                                                             ConsumerConfigurationData<?> conf) {
        return ofTopicName(topicName, conf.getPriorityLevel());
    }

    static TopicConsumerConfigurationData of(@NonNull TopicNameMatcher topicNameMatcher, int priorityLevel) {
        return new TopicConsumerConfigurationData(topicNameMatcher, priorityLevel);
    }

    public interface TopicNameMatcher extends Serializable {
        boolean matches(String topicName);

        @RequiredArgsConstructor
        class TopicsPattern implements TopicNameMatcher {
            @NonNull
            private final Pattern topicsPattern;

            @Override
            public boolean matches(String topicName) {
                return topicsPattern.matcher(topicName).matches();
            }
        }

        @RequiredArgsConstructor
        class TopicName implements TopicNameMatcher {
            @NonNull
            private final String topicName;

            @Override
            public boolean matches(String topicName) {
                return this.topicName.equals(topicName);
            }
        }
    }
}
