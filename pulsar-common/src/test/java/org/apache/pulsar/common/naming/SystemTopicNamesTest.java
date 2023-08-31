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
package org.apache.pulsar.common.naming;

import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class SystemTopicNamesTest {

    @DataProvider(name = "topicPoliciesSystemTopicNames")
    public static Object[][] topicPoliciesSystemTopicNames() {
        return new Object[][] {
                {"persistent://public/default/__change_events", true},
                {"persistent://public/default/__change_events-partition-0", true},
                {"persistent://random-tenant/random-ns/__change_events", true},
                {"persistent://random-tenant/random-ns/__change_events-partition-1", true},
                {"persistent://public/default/not_really__change_events", false},
                {"persistent://public/default/__change_events-diff-suffix", false},
                {"persistent://a/b/not_really__change_events", false},
        };
    }

    @Test(dataProvider = "topicPoliciesSystemTopicNames")
    public void testIsTopicPoliciesSystemTopic(String topicName, boolean expectedResult) {
        assertEquals(expectedResult, SystemTopicNames.isTopicPoliciesSystemTopic(topicName));
        assertEquals(expectedResult, SystemTopicNames.isSystemTopic(TopicName.get(topicName)));
        assertEquals(expectedResult, SystemTopicNames.isEventSystemTopic(TopicName.get(topicName)));
    }
}
