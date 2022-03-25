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
package org.apache.pulsar.client.cli;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCmdConsume {

    CmdConsume cmdConsume;

    @BeforeMethod
    public void setUp() throws Exception {
        cmdConsume = new CmdConsume();
        cmdConsume.updateConfig(null, null, "ws://localhost:8080/");
        Field subscriptionNameField = CmdConsume.class.getDeclaredField("subscriptionName");
        subscriptionNameField.setAccessible(true);
        subscriptionNameField.set(cmdConsume, "my-sub");
    }

    @Test
    public void testGetWebSocketConsumeUri() {
        String topicNameV1 = "persistent://public/cluster/default/issue-11067";
        assertEquals(cmdConsume.getWebSocketConsumeUri(topicNameV1),
                "ws://localhost:8080/ws/consumer/persistent/public/cluster/default/issue-11067/my-sub"
                        + "?subscriptionType=Exclusive&subscriptionMode=Durable");
        String topicNameV2 = "persistent://public/default/issue-11067";
        assertEquals(cmdConsume.getWebSocketConsumeUri(topicNameV2),
                "ws://localhost:8080/ws/v2/consumer/persistent/public/default/issue-11067/my-sub"
                        + "?subscriptionType=Exclusive&subscriptionMode=Durable");
    }

}
