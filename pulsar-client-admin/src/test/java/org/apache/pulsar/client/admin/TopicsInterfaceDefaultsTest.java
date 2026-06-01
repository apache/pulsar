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
package org.apache.pulsar.client.admin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TransactionIsolationLevel;
import org.testng.annotations.Test;

/**
 * Unit tests verifying that the default-method overloads of
 * {@link Topics#peekMessages} and {@link Topics#peekMessagesAsync} delegate correctly to the
 * primary abstract method
 * {@code peekMessages(topic, sub, messagePosition, numMessages, showServerMarker, isolation)}.
 *
 * <p>Uses Mockito with {@code CALLS_REAL_METHODS} so the interface defaults run for real while
 * the primary abstract method is stubbed.
 *
 * <p>Tests added for PIP-332.
 */
public class TopicsInterfaceDefaultsTest {

    private Topics mockTopicsWithStubbedPrimary() {
        Topics topics = mock(Topics.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        // Stub the sync primary
        try {
            doReturn(Collections.<Message<byte[]>>emptyList())
                    .when(topics)
                    .peekMessages(anyString(), anyString(), anyInt(), anyInt(),
                            anyBoolean(), any(TransactionIsolationLevel.class));
        } catch (Exception ignored) {
            // checked exception declared on the method; no real call here
        }
        // Stub the async primary
        doReturn(CompletableFuture.completedFuture(Collections.<Message<byte[]>>emptyList()))
                .when(topics)
                .peekMessagesAsync(anyString(), anyString(), anyInt(), anyInt(),
                        anyBoolean(), any(TransactionIsolationLevel.class));
        return topics;
    }

    @Test
    public void threeArgPeekDelegatesWithMessagePositionOne() throws Exception {
        Topics topics = mockTopicsWithStubbedPrimary();
        topics.peekMessages("topic", "sub", 10);
        verify(topics).peekMessages("topic", "sub", 1, 10, false,
                TransactionIsolationLevel.READ_COMMITTED);
    }

    @Test
    public void fourArgPeekDelegatesWithSuppliedPosition() throws Exception {
        Topics topics = mockTopicsWithStubbedPrimary();
        topics.peekMessages("topic", "sub", 91, 10);
        verify(topics).peekMessages("topic", "sub", 91, 10, false,
                TransactionIsolationLevel.READ_COMMITTED);
    }

    @Test
    public void fiveArgPeekDelegatesWithMessagePositionOne() throws Exception {
        Topics topics = mockTopicsWithStubbedPrimary();
        topics.peekMessages("topic", "sub", 10, true, TransactionIsolationLevel.READ_UNCOMMITTED);
        verify(topics).peekMessages("topic", "sub", 1, 10, true,
                TransactionIsolationLevel.READ_UNCOMMITTED);
    }

    @Test
    public void threeArgPeekAsyncDelegatesWithMessagePositionOne() {
        Topics topics = mockTopicsWithStubbedPrimary();
        CompletableFuture<List<Message<byte[]>>> fut = topics.peekMessagesAsync("topic", "sub", 10);
        verify(topics).peekMessagesAsync("topic", "sub", 1, 10, false,
                TransactionIsolationLevel.READ_COMMITTED);
        // Smoke: the stub completes with an empty list
        fut.join();
    }

    @Test
    public void fourArgPeekAsyncDelegatesWithSuppliedPosition() {
        Topics topics = mockTopicsWithStubbedPrimary();
        topics.peekMessagesAsync("topic", "sub", 91, 10);
        verify(topics).peekMessagesAsync("topic", "sub", 91, 10, false,
                TransactionIsolationLevel.READ_COMMITTED);
    }

    @Test
    public void fiveArgPeekAsyncDelegatesWithMessagePositionOne() {
        Topics topics = mockTopicsWithStubbedPrimary();
        topics.peekMessagesAsync("topic", "sub", 10, true, TransactionIsolationLevel.READ_UNCOMMITTED);
        verify(topics).peekMessagesAsync("topic", "sub", 1, 10, true,
                TransactionIsolationLevel.READ_UNCOMMITTED);
    }
}
