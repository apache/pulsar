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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.re2j.Pattern;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@UtilityClass
public class TopicList {
    public static final String ALL_TOPICS_PATTERN = ".*";

    private static final String SCHEME_SEPARATOR = "://";

    private static final Pattern SCHEME_SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(SCHEME_SEPARATOR));

    public static List<String> filterTopics(List<String> original, java.util.regex.Pattern jdkPattern) {
        return filterTopics(original, TopicsPatternFactory.create(jdkPattern));
    }

    // get topics that match 'topicsPattern' from original topics list
    // matching is performed with base topic names, without partition part
    public static List<String> filterTopics(List<String> original, String regex,
                                            TopicsPattern.RegexImplementation topicsPatternImplementation) {
        return filterTopics(original, TopicsPatternFactory.create(regex, topicsPatternImplementation));
    }

    /**
     * Filter topics using a TopicListPattern instance.
     */
    public static List<String> filterTopics(List<String> original, TopicsPattern topicsPattern) {
        return filterTopics(original, topicsPattern, Collectors.toList());
    }

    /**
     * Filter topics using a TopicListPattern instance and collect the results using a specified collector.
     */
    public static <R> R filterTopics(List<String> original, TopicsPattern topicsPattern,
                                              Collector<String, ?, R> collector) {
        return filterTopicsToStream(original, topicsPattern)
                .collect(collector);
    }

    /**
     * Filter topics using a TopicListPattern instance and return a stream of filtered topic names.
     */
    public static Stream<String> filterTopicsToStream(List<String> original, TopicsPattern topicsPattern) {
        return original.stream()
                .map(TopicName::get)
                .filter(topicName -> {
                    String partitionedTopicName = topicName.getPartitionedTopicName();
                    String removedScheme = SCHEME_SEPARATOR_PATTERN.split(partitionedTopicName)[1];
                    return topicsPattern.matches(removedScheme);
                })
                .map(TopicName::toString);
    }

    public static List<String> filterSystemTopic(List<String> original) {
        return original.stream()
                .filter(topic -> !SystemTopicNames.isSystemTopic(TopicName.get(topic)))
                .collect(Collectors.toList());
    }

    public static String calculateHash(Collection<String> topics) {
        Hasher hasher = Hashing.crc32c().newHasher();
        String[] sortedTopics = topics.toArray(new String[topics.size()]);
        Arrays.sort(sortedTopics);
        for (int i = 0; i < sortedTopics.length; i++) {
            hasher.putString(sortedTopics[i], StandardCharsets.UTF_8);
            // Skip the delimiter for the last item so that the hash format is compatible with previous versions
            if (i < sortedTopics.length - 1) {
                hasher.putByte((byte) ',');
            }
        }
        return hasher.hash().toString();
    }

    // get topics, which are contained in list1, and not in list2
    public static Set<String> minus(Collection<String> list1, Collection<String> list2) {
        HashSet<String> s1 = new HashSet<>(list1);
        s1.removeAll(list2);
        return s1;
    }

    public static String removeTopicDomainScheme(String originalRegexp) {
        if (!originalRegexp.toString().contains(SCHEME_SEPARATOR)) {
            return originalRegexp;
        }
        String[] parts = SCHEME_SEPARATOR_PATTERN.split(originalRegexp.toString());
        String prefix = parts[0];
        String removedTopicDomain = parts[1];
        if (prefix.equals(TopicDomain.persistent.value()) || prefix.equals(TopicDomain.non_persistent.value())) {
            prefix = "";
        } else if (prefix.endsWith(TopicDomain.non_persistent.value())) {
            prefix = prefix.substring(0, prefix.length() - TopicDomain.non_persistent.value().length());
        } else if (prefix.endsWith(TopicDomain.persistent.value())){
            prefix = prefix.substring(0, prefix.length() - TopicDomain.persistent.value().length());
        } else {
            throw new IllegalArgumentException("Does not support topic domain: " + prefix);
        }
        return prefix + removedTopicDomain;
    }
}
