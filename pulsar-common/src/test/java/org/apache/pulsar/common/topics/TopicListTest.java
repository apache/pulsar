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

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TopicListTest {

    @Test
    public void testFilterTopics() {
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1";
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2";
        String topicName3 = "persistent://my-property/my-ns/hello-3";
        String topicName4 = "non-persistent://my-property/my-ns/hello-4";

        List<String> topicsNames = Lists.newArrayList(topicName1, topicName2, topicName3, topicName4);

        Pattern pattern1 = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");
        List<String> result1 = TopicList.filterTopics(topicsNames, pattern1);
        assertTrue(result1.size() == 2 && result1.contains(topicName1) && result1.contains(topicName2));

        Pattern pattern2 = Pattern.compile("persistent://my-property/my-ns/.*");
        List<String> result2 = TopicList.filterTopics(topicsNames, pattern2);
        assertTrue(result2.size() == 4
                && Stream.of(topicName1, topicName2, topicName3, topicName4).allMatch(result2::contains));
    }

    @Test
    public void testMinus() {
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1";
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2";
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3";
        String topicName4 = "persistent://my-property/my-ns/pattern-topic-4";
        String topicName5 = "persistent://my-property/my-ns/pattern-topic-5";
        String topicName6 = "persistent://my-property/my-ns/pattern-topic-6";

        List<String> oldNames = Lists.newArrayList(topicName1, topicName2, topicName3, topicName4);
        List<String> newNames = Lists.newArrayList(topicName3, topicName4, topicName5, topicName6);

        Set<String> addedNames = TopicList.minus(newNames, oldNames);
        Set<String> removedNames = TopicList.minus(oldNames, newNames);

        assertTrue(addedNames.size() == 2 &&
                addedNames.contains(topicName5) &&
                addedNames.contains(topicName6));
        assertTrue(removedNames.size() == 2 &&
                removedNames.contains(topicName1) &&
                removedNames.contains(topicName2));

        // totally 2 different list, should return content of first lists.
        Set<String> addedNames2 = TopicList.minus(addedNames, removedNames);
        assertTrue(addedNames2.size() == 2 &&
                addedNames2.contains(topicName5) &&
                addedNames2.contains(topicName6));

        // 2 same list, should return empty list.
        Set<String> addedNames3 = TopicList.minus(addedNames, addedNames);
        assertEquals(addedNames3.size(), 0);

        // empty list minus: addedNames2.size = 2, addedNames3.size = 0
        Set<String> addedNames4 = TopicList.minus(addedNames2, addedNames3);
        assertEquals(addedNames2.size(), addedNames4.size());
        addedNames4.forEach(name -> assertTrue(addedNames2.contains(name)));

        Set<String> addedNames5 = TopicList.minus(addedNames3, addedNames2);
        assertEquals(addedNames5.size(), 0);
    }

    @Test
    public void testCalculateHash() {
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1";
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2";
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3";
        String hash1 = TopicList.calculateHash(Arrays.asList(topicName3, topicName2, topicName1));
        String hash2 = TopicList.calculateHash(Arrays.asList(topicName1, topicName3, topicName2));
        assertEquals(hash1, hash2, "Hash must not depend on order of topics in the list");

        String hash3 = TopicList.calculateHash(Arrays.asList(topicName1, topicName2));
        assertNotEquals(hash1, hash3, "Different list must have different hashes");

    }


}
