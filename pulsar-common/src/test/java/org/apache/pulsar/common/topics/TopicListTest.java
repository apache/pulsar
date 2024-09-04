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

import com.google.common.collect.Lists;
import com.google.re2j.Pattern;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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

    @Test
    public void testRemoveTopicDomainScheme() {
        // persistent.
        final String tpName1 = "persistent://public/default/tp";
        String res1 = TopicList.removeTopicDomainScheme(tpName1);
        assertEquals(res1, "public/default/tp");

        // non-persistent
        final String tpName2 = "non-persistent://public/default/tp";
        String res2 = TopicList.removeTopicDomainScheme(tpName2);
        assertEquals(res2, "public/default/tp");

        // without topic domain.
        final String tpName3 = "public/default/tp";
        String res3 = TopicList.removeTopicDomainScheme(tpName3);
        assertEquals(res3, "public/default/tp");

        // persistent & "java.util.regex.Pattern.quote".
        final String tpName4 = java.util.regex.Pattern.quote(tpName1);
        String res4 = TopicList.removeTopicDomainScheme(tpName4);
        assertEquals(res4, java.util.regex.Pattern.quote("public/default/tp"));

        // persistent & "java.util.regex.Pattern.quote" & "^$".
        final String tpName5 = "^" + java.util.regex.Pattern.quote(tpName1) + "$";
        String res5 = TopicList.removeTopicDomainScheme(tpName5);
        assertEquals(res5, "^" + java.util.regex.Pattern.quote("public/default/tp") + "$");

        // persistent & "com.google.re2j.Pattern.quote".
        final String tpName6 = Pattern.quote(tpName1);
        String res6 = TopicList.removeTopicDomainScheme(tpName6);
        assertEquals(res6, Pattern.quote("public/default/tp"));

        // non-persistent & "java.util.regex.Pattern.quote".
        final String tpName7 = java.util.regex.Pattern.quote(tpName2);
        String res7 = TopicList.removeTopicDomainScheme(tpName7);
        assertEquals(res7, java.util.regex.Pattern.quote("public/default/tp"));

        // non-persistent & "com.google.re2j.Pattern.quote".
        final String tpName8 = Pattern.quote(tpName2);
        String res8 = TopicList.removeTopicDomainScheme(tpName8);
        assertEquals(res8, Pattern.quote("public/default/tp"));

        // non-persistent & "com.google.re2j.Pattern.quote" & "^$".
        final String tpName9 = "^" + Pattern.quote(tpName2) + "$";
        String res9 = TopicList.removeTopicDomainScheme(tpName9);
        assertEquals(res9, "^" + Pattern.quote("public/default/tp") + "$");

        // wrong topic domain.
        final String tpName10 = "xx://public/default/tp";
        try {
            TopicList.removeTopicDomainScheme(tpName10);
            fail("Does not support the topic domain xx");
        } catch (Exception ex) {
            // expected error.
        }
    }
}
