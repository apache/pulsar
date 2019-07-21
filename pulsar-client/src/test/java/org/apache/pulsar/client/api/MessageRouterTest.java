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
package org.apache.pulsar.client.api;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * Unit test of {@link MessageRouter}.
 */
public class MessageRouterTest {

    private static class TestMessageRouter implements MessageRouter {

        @Override
        public int choosePartition(Message<?> msg) {
            return 1234;
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testChoosePartition() {
        MessageRouter router = spy(new TestMessageRouter());
        Message<?> mockedMsg = mock(Message.class);
        TopicMetadata mockedMetadata = mock(TopicMetadata.class);

        assertEquals(1234, router.choosePartition(mockedMsg));
        assertEquals(1234, router.choosePartition(mockedMsg, mockedMetadata));

        verify(router, times(2)).choosePartition(eq(mockedMsg));
    }

}
