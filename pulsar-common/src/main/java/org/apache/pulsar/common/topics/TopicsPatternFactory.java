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
package org.apache.pulsar.common.topics;

import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory class for creating instances of TopicsPattern based on different regex implementations.
 * It supports JDK regex, RE2J regex, and a fallback mechanism for RE2J with JDK.
 */
@UtilityClass
@Slf4j
public class TopicsPatternFactory {
    /**
     * Creates a TopicsPattern from a JDK Pattern.
     * If the pattern contains the topic domain scheme, it will be removed and a RE2J_WITH_JDK_FALLBACK implementation
     * will be used. If the pattern does not contain the topic domain scheme, the input JDK Pattern instance will be
     * used to evaluate topic name matches.
     * The Pulsar Java Client will use this method to create a TopicsPattern from a JDK Pattern.
     *
     * @param jdkPattern the JDK Pattern to create the TopicsPattern from
     * @return a TopicsPattern instance
     */
    public static TopicsPattern create(Pattern jdkPattern) {
        String currentRegex = jdkPattern.pattern();
        String removedTopicDomainScheme = TopicList.removeTopicDomainScheme(currentRegex);
        // If the regex pattern is already without the topic domain scheme, we can directly create a JDKTopicsPattern
        if (currentRegex.equals(removedTopicDomainScheme)) {
            return new JDKTopicsPattern(jdkPattern);
        } else {
            // If the regex pattern contains the topic domain scheme, we remove it and create a TopicsPattern
            // using RE2J_WITH_JDK_FALLBACK
            return internalCreate(currentRegex, removedTopicDomainScheme,
                    TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK);
        }
    }

    /**
     * Creates a TopicsPattern from a regex string, using the specified regex implementation.
     * The Pulsar Broker will use this method to create a TopicsPattern from a regex string. The implementation
     * is configured in the broker configuration file with the `topicsPatternRegexImplementation` property.
     *
     * @param regex the regex string to create the TopicsPattern from
     * @param implementation the regex implementation to use (RE2J, JDK, or RE2J_WITH_JDK_FALLBACK)
     * @return a TopicsPattern instance
     */
    public static TopicsPattern create(String regex, TopicsPattern.RegexImplementation implementation) {
        return internalCreate(regex, TopicList.removeTopicDomainScheme(regex), implementation);
    }

    /**
     * Creates a TopicsPattern that matches all topics.
     *
     * @return a TopicsPattern instance that matches all topics
     */
    public static TopicsPattern matchAll() {
        return MatchAllTopicsPattern.INSTANCE;
    }

    private static TopicsPattern internalCreate(String inputPattern, String regexWithoutTopicDomainScheme,
                                                TopicsPattern.RegexImplementation implementation) {
        if (TopicList.ALL_TOPICS_PATTERN.equals(regexWithoutTopicDomainScheme)) {
            return matchAll();
        }
        switch (implementation) {
            case RE2J:
                return new RE2JTopicsPattern(inputPattern, regexWithoutTopicDomainScheme);
            case JDK:
                return new JDKTopicsPattern(inputPattern, regexWithoutTopicDomainScheme);
            case RE2J_WITH_JDK_FALLBACK:
                try {
                    return new RE2JTopicsPattern(inputPattern, regexWithoutTopicDomainScheme);
                } catch (com.google.re2j.PatternSyntaxException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to compile regex pattern '{}' with RE2J, fallback to JDK",
                                regexWithoutTopicDomainScheme, e);
                    }
                    // Fallback to JDK implementation if RE2J fails
                    return new JDKTopicsPattern(inputPattern, regexWithoutTopicDomainScheme);
                }
            default:
                throw new IllegalArgumentException("Unknown RegexImplementation: " + implementation);
        }
    }
}
