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
package org.apache.pulsar.client.impl;

import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.Assert;
import org.junit.Test;

public class ProducerImplTest {
    @Test
    public void testChunkedMessageCtxDeallocate() {
        int totalChunks = 3;
        ProducerImpl.ChunkedMessageCtx ctx = ProducerImpl.ChunkedMessageCtx.get(totalChunks);
        MessageIdImpl testMessageId = new MessageIdImpl(1, 1, 1);
        ctx.firstChunkMessageId = testMessageId;

        for (int i = 0; i < totalChunks; i++) {
            ProducerImpl.OpSendMsg opSendMsg =
                    ProducerImpl.OpSendMsg.create(
                            MessageImpl.create(new MessageMetadata(), ByteBuffer.allocate(0), Schema.STRING, null),
                            null, 0, null);
            opSendMsg.chunkedMessageCtx = ctx;
            // check the ctx hasn't been deallocated.
            Assert.assertEquals(testMessageId, ctx.firstChunkMessageId);
            opSendMsg.recycle();
        }

        // check if the ctx is deallocated successfully.
        Assert.assertNull(ctx.firstChunkMessageId);
    }
}
