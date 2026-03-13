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
package org.apache.pulsar.broker.admin;

import java.io.IOException;
import java.util.List;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SkipMessageIdsRequestTest {

    @Test
    public void testDeserializeMessageIdListFormat() throws Exception {
        String json = "{\"type\":\"messageId\",\"messageIds\":["
                + "{\"ledgerId\":123,\"entryId\":45},"
                + "{\"ledgerId\":\"124\",\"entryId\":\"46\",\"batchIndex\":2}"
                + "]}";
        SkipMessageIdsRequest req = ObjectMapperFactory.getMapper()
                .reader()
                .forType(SkipMessageIdsRequest.class)
                .readValue(json);

        List<SkipMessageIdsRequest.MessageIdItem> items = req.getItems();
        Assert.assertEquals(items.size(), 2);
        Assert.assertTrue(items.stream()
                .anyMatch(i -> i.getLedgerId() == 123L && i.getEntryId() == 45L && i.getBatchIndex() == null));
        Assert.assertTrue(items.stream()
                .anyMatch(i -> i.getLedgerId() == 124L && i.getEntryId() == 46L
                        && Integer.valueOf(2).equals(i.getBatchIndex())));
    }

    @Test
    public void testRejectObjectMessageIds() {
        String json = "{\"type\":\"messageId\",\"messageIds\":{\"ledgerId\":123,\"entryId\":45}}";
        Assert.expectThrows(IOException.class, () -> ObjectMapperFactory.getMapper()
                .reader()
                .forType(SkipMessageIdsRequest.class)
                .readValue(json));
    }

    @Test
    public void testRejectMessageIdListMissingLedgerId() {
        String json = "{\"type\":\"messageId\",\"messageIds\":[{\"entryId\":45}]}";
        Assert.expectThrows(IOException.class, () -> ObjectMapperFactory.getMapper()
                .reader()
                .forType(SkipMessageIdsRequest.class)
                .readValue(json));
    }

    @Test
    public void testRejectInvalidBase64() {
        String json = "{\"type\":\"byteArray\",\"messageIds\":[\"not_base64!!\"]}";
        Assert.expectThrows(IOException.class, () -> ObjectMapperFactory.getMapper()
                .reader()
                .forType(SkipMessageIdsRequest.class)
                .readValue(json));
    }
}
