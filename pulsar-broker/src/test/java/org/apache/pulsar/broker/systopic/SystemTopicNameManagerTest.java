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
import java.util.UUID;

public class SystemTopicNameManagerTest {

    @Test
    public void testIsSystemTopic() {
        assertFalse(SystemTopicNameManager.isSystemTopic(TopicName.get("test")));
        assertFalse(SystemTopicNameManager.isSystemTopic(TopicName.get("public/default/test")));
        assertFalse(SystemTopicNameManager.isSystemTopic(TopicName.get("persistent://public/default/test")));
        assertFalse(SystemTopicNameManager.isSystemTopic(TopicName.get("non-persistent://public/default/test")));

        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get("__change_events")));
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get("__change_events-partition-0")));
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get("__change_events-partition-1")));
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get("__transaction_buffer_snapshot")));
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get("__transaction_buffer_snapshot-partition-0")));
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get("__transaction_buffer_snapshot-partition-1")));
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName
                .get("topicxxx-partition-0-multiTopicsReader-f433329d68__transaction_pending_ack")));
        assertTrue(SystemTopicNameManager.isSystemTopic(
                TopicName.get("topicxxx-multiTopicsReader-f433329d68__transaction_pending_ack")));

        assertTrue(SystemTopicNameManager.isSystemTopic(TRANSACTION_COORDINATOR_ASSIGN));
        assertTrue(SystemTopicNameManager.isSystemTopic(TRANSACTION_COORDINATOR_LOG));
    }

    @Test
    public void testRegister(){
        String sysName = "__test__system__topic";
        SystemTopicNameManager.register(sysName);
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get(sysName)));
    }


    @Test
    public void testRegisterWithPolicy(){
        String sysName = "__test__system__topic";
        SystemTopicNameManager.register(sysName, SystemTopicNamePolicy.FULL_QUALITY);
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get(sysName)));

        String suffixSystemName = "__test__sys_suffix";
        SystemTopicNameManager.register(suffixSystemName, SystemTopicNamePolicy.SUFFIX);
        String suffixUUIDName = UUID.randomUUID() + suffixSystemName;
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get(suffixUUIDName)));

        String prefixSystemName = "__test__sys_prefix";
        SystemTopicNameManager.register(prefixSystemName, SystemTopicNamePolicy.PREFIX);
        String prefixUUIDName =  prefixSystemName + UUID.randomUUID();
        assertTrue(SystemTopicNameManager.isSystemTopic(TopicName.get(prefixUUIDName)));
    }

}
