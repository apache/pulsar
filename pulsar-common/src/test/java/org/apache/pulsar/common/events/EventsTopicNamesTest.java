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
package org.apache.pulsar.common.events;

import static org.testng.AssertJUnit.assertEquals;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class EventsTopicNamesTest {

    @DataProvider(name = "topicPoliciesEventsTopicNames")
    public static Object[][] topicPoliciesEventsTopicNames() {
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

    @Test(dataProvider = "topicPoliciesEventsTopicNames")
    public void testIsTopicPoliciesSystemTopic(String topicName, boolean expectedResult) {
        assertEquals(expectedResult, EventsTopicNames.isTopicPoliciesSystemTopic(topicName));
        assertEquals(expectedResult, EventsTopicNames.checkTopicIsEventsNames(TopicName.get(topicName)));
    }
}
