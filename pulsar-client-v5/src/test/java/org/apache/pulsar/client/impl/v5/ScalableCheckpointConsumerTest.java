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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.TopicMessageId;
import org.testng.annotations.Test;

/**
 * The reader a {@link ScalableCheckpointConsumer} opens to resolve a latest start position is not tracked with
 * the segment readers, so the lookup must not complete before that reader is closed.
 */
public class ScalableCheckpointConsumerTest {

    private static final List<TopicMessageId> LAST_MESSAGE_IDS =
            List.of(TopicMessageId.create("segment://public/default/topic/0000-ffff-0", MessageId.earliest));

    @Test
    public void testLookupCompletesOnceTheReaderIsClosed() {
        Reader<?> lookupReader = mock(Reader.class);
        when(lookupReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(LAST_MESSAGE_IDS));
        CompletableFuture<Void> closing = new CompletableFuture<>();
        when(lookupReader.closeAsync()).thenReturn(closing);

        var lookup = ScalableCheckpointConsumer.lastMessageIdsThenCloseAsync(lookupReader);

        verify(lookupReader).closeAsync();
        assertThat(lookup).as("lookup before the reader is closed").isNotDone();
        closing.complete(null);
        assertThat(lookup).isCompletedWithValue(LAST_MESSAGE_IDS);
    }

    @Test
    public void testFailedLookupFailsOnceTheReaderIsClosed() {
        Reader<?> lookupReader = mock(Reader.class);
        var lookupError = new PulsarClientException("lookup failed");
        when(lookupReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.failedFuture(lookupError));
        CompletableFuture<Void> closing = new CompletableFuture<>();
        when(lookupReader.closeAsync()).thenReturn(closing);

        var lookup = ScalableCheckpointConsumer.lastMessageIdsThenCloseAsync(lookupReader);

        verify(lookupReader).closeAsync();
        assertThat(lookup).as("lookup before the reader is closed").isNotDone();
        // The lookup's error is kept, not the close's.
        closing.completeExceptionally(new PulsarClientException("close failed"));
        assertThatThrownBy(lookup::join).isInstanceOf(CompletionException.class).cause().isSameAs(lookupError);
    }

    @Test
    public void testFailedCloseKeepsTheLookupResult() {
        Reader<?> lookupReader = mock(Reader.class);
        when(lookupReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(LAST_MESSAGE_IDS));
        when(lookupReader.closeAsync())
                .thenReturn(CompletableFuture.failedFuture(new PulsarClientException("close failed")));

        var lookup = ScalableCheckpointConsumer.lastMessageIdsThenCloseAsync(lookupReader);

        assertThat(lookup).isCompletedWithValue(LAST_MESSAGE_IDS);
    }
}
