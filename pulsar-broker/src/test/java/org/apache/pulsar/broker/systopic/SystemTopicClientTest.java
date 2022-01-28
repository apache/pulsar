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
package org.apache.pulsar.broker.systopic;

import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_ASSIGN;
import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_LOG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class SystemTopicClientTest {

    @Test
    public void testIsSystemTopic() {
        assertFalse(SystemTopicClient.isSystemTopic(TopicName.get("test")));
        assertFalse(SystemTopicClient.isSystemTopic(TopicName.get("public/default/test")));
        assertFalse(SystemTopicClient.isSystemTopic(TopicName.get("persistent://public/default/test")));
        assertFalse(SystemTopicClient.isSystemTopic(TopicName.get("non-persistent://public/default/test")));

        assertTrue(SystemTopicClient.isSystemTopic(TopicName.get("__change_events")));
        assertTrue(SystemTopicClient.isSystemTopic(TopicName.get("__change_events-partition-0")));
        assertTrue(SystemTopicClient.isSystemTopic(TopicName.get("__change_events-partition-1")));
        assertTrue(SystemTopicClient.isSystemTopic(TopicName.get("__transaction_buffer_snapshot")));
        assertTrue(SystemTopicClient.isSystemTopic(TopicName.get("__transaction_buffer_snapshot-partition-0")));
        assertTrue(SystemTopicClient.isSystemTopic(TopicName.get("__transaction_buffer_snapshot-partition-1")));
        assertTrue(SystemTopicClient.isSystemTopic(TopicName
                .get("topicxxx-partition-0-multiTopicsReader-f433329d68__transaction_pending_ack")));
        assertTrue(SystemTopicClient.isSystemTopic(
                TopicName.get("topicxxx-multiTopicsReader-f433329d68__transaction_pending_ack")));

        assertTrue(SystemTopicClient.isSystemTopic(TRANSACTION_COORDINATOR_ASSIGN));
        assertTrue(SystemTopicClient.isSystemTopic(TRANSACTION_COORDINATOR_LOG));
    }
}
