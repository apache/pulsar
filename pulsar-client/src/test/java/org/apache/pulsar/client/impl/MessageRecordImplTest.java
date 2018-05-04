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

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.MessageId;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MessageRecordImplTest {

    private MessageRecordImpl<byte[], MessageId> message;
    private MessageId messageId;

    @BeforeMethod
    public void setup() {
        this.messageId = mock(MessageId.class);
        this.message = mock(MessageRecordImpl.class, CALLS_REAL_METHODS);
        when(message.getMessageId()).thenReturn(messageId);
    }

    @Test
    public void testAck() throws Exception {
        final AtomicReference<MessageId> msgIdRef = new AtomicReference<>();
        final CountDownLatch ackLatch = new CountDownLatch(1);

        this.message.setAckFunction(msgId -> {
            msgIdRef.set(msgId);
            ackLatch.countDown();
        });

        this.message.ack();
        ackLatch.await();

        assertSame(messageId, msgIdRef.get());
    }

}
