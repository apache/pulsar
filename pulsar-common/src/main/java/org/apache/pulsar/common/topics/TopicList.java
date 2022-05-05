/**
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

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.common.naming.TopicName;

@UtilityClass
public class TopicList {

    private static final String SCHEME_SEPARATOR = "://";

    private static final Pattern SCHEME_SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(SCHEME_SEPARATOR));

    // get topics that match 'topicsPattern' from original topics list
    // return result should contain only topic names, without partition part
    public static List<String> filterTopics(List<String> original, String regex) {
        Pattern topicsPattern = Pattern.compile(regex);
        return filterTopics(original, topicsPattern);
    }
    public static List<String> filterTopics(List<String> original, Pattern topicsPattern) {

        final Pattern shortenedTopicsPattern = topicsPattern.toString().contains(SCHEME_SEPARATOR)
                ? Pattern.compile(SCHEME_SEPARATOR_PATTERN.split(topicsPattern.toString())[1]) : topicsPattern;

        return original.stream()
                .map(TopicName::get)
                .map(TopicName::toString)
                .filter(topic -> shortenedTopicsPattern.matcher(SCHEME_SEPARATOR_PATTERN.split(topic)[1]).matches())
                .collect(Collectors.toList());
    }

    public static String calculateHash(List<String> topics) {
        return Hashing.crc32c().hashBytes(topics.stream()
                .sorted()
                .collect(Collectors.joining(","))
                .getBytes(StandardCharsets.UTF_8)).toString();
    }



    // get topics, which are contained in list1, and not in list2
    public static Set<String> minus(Collection<String> list1, Collection<String> list2) {
        HashSet<String> s1 = new HashSet<>(list1);
        s1.removeAll(list2);
        return s1;
    }
}
