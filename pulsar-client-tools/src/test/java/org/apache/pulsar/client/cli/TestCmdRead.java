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
package org.apache.pulsar.client.cli;

import static org.testng.Assert.assertEquals;
import java.lang.reflect.Field;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestCmdRead {

    @DataProvider(name = "startMessageIds")
    public Object[][] startMessageIds() {
        // The V5-based reader (CheckpointConsumer) only accepts latest / earliest; the
        // <ledgerId>:<entryId> form is no longer supported, so it is not exercised here.
        return new Object[][] {
            { "latest", "latest" },
            { "earliest", "earliest" },
        };
    }

    @Test(dataProvider = "startMessageIds")
    public void testGetWebSocketReadUri(String msgId, String msgIdQueryParam) throws Exception {
        CmdRead cmdRead = new CmdRead();
        cmdRead.updateConfig(null, null, "ws://localhost:8080/");
        Field startMessageIdField = CmdRead.class.getDeclaredField("startMessageId");
        startMessageIdField.setAccessible(true);
        startMessageIdField.set(cmdRead, msgId);

        String topicNameV2 = "persistent://public/default/t2";
        assertEquals(cmdRead.getWebSocketReadUri(topicNameV2),
                "ws://localhost:8080/ws/v2/reader/persistent/public/default/t2?messageId=" + msgIdQueryParam);
    }
}
