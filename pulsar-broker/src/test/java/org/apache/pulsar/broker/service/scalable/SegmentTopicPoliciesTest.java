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
package org.apache.pulsar.broker.service.scalable;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SegmentTopicPoliciesTest {

    @Test
    public void testTopicPoliciesName() {
        TopicName parent = TopicName.get("topic://t/ns/x");
        TopicName segment = SegmentTopicName.fromParent(parent, HashRange.of(0, 0xffff), 0);
        assertEquals(SegmentTopicPolicies.topicPoliciesName(segment), parent);
        assertEquals(SegmentTopicPolicies.topicPoliciesName(TopicName.get("persistent://t/ns/x-partition-3")),
                TopicName.get("persistent://t/ns/x"));
        assertEquals(SegmentTopicPolicies.topicPoliciesName(TopicName.get("persistent://t/ns/x")),
                TopicName.get("persistent://t/ns/x"));
        assertEquals(SegmentTopicPolicies.topicPoliciesName(parent), parent);
    }
}
