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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Lists;
import java.util.Arrays;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Contains unit tests for ProducerImpl.OpSendMsgQueue inner class.
 */
public class OpSendMsgQueueTest {
    MessageImpl<?> message;

    @BeforeClass
    public void createMockMessage() {
        message = mock(MessageImpl.class);
        when(message.getUncompressedSize()).thenReturn(0);
    }

    private ProducerImpl.OpSendMsg createDummyOpSendMsg() {
        return ProducerImpl.OpSendMsg.create(message, null, 0L, null);
    }

    @Test
    public void shouldPostponeAddsToPreventConcurrentModificationException() {
        // given
        ProducerImpl.OpSendMsgQueue queue = new ProducerImpl.OpSendMsgQueue();
        ProducerImpl.OpSendMsg opSendMsg = createDummyOpSendMsg();
        ProducerImpl.OpSendMsg opSendMsg2 = createDummyOpSendMsg();
        queue.add(opSendMsg);

        // when
        queue.forEach(item -> {
            queue.add(opSendMsg2);
        });

        // then
        assertEquals(Lists.newArrayList(queue), Arrays.asList(opSendMsg, opSendMsg2));
    }

    @Test
    public void shouldPostponeAddsAlsoInRecursiveCalls() {
        // given
        ProducerImpl.OpSendMsgQueue queue = new ProducerImpl.OpSendMsgQueue();
        ProducerImpl.OpSendMsg opSendMsg = createDummyOpSendMsg();
        ProducerImpl.OpSendMsg opSendMsg2 = createDummyOpSendMsg();
        ProducerImpl.OpSendMsg opSendMsg3 = createDummyOpSendMsg();
        ProducerImpl.OpSendMsg opSendMsg4 = createDummyOpSendMsg();
        queue.add(opSendMsg);

        // when
        queue.forEach(item -> {
            queue.add(opSendMsg2);
            // recursive forEach
            queue.forEach(item2 -> {
                queue.add(opSendMsg3);
            });
            queue.add(opSendMsg4);
        });

        // then
        assertEquals(Lists.newArrayList(queue), Arrays.asList(opSendMsg, opSendMsg2, opSendMsg3, opSendMsg4));
    }
}
