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
package org.apache.pulsar.functions.instance;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

public class ProducerCacheTest {

    @Test
    public void shouldTolerateAlreadyClosedExceptionInClose() {
        ProducerCache cache = new ProducerCache();
        Producer producer = mock(Producer.class);
        when(producer.flushAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(producer.closeAsync()).thenReturn(
                CompletableFuture.failedFuture(new PulsarClientException.AlreadyClosedException("Already closed")));
        cache.getOrCreateProducer(ProducerCache.CacheArea.CONTEXT_CACHE, "topic", "key",
                () -> (Producer<Object>) producer);
        cache.close();
    }

    @Test
    public void shouldTolerateRuntimeExceptionInClose() {
        ProducerCache cache = new ProducerCache();
        Producer producer = mock(Producer.class);
        when(producer.flushAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(producer.closeAsync()).thenThrow(new RuntimeException("Some exception"));
        cache.getOrCreateProducer(ProducerCache.CacheArea.CONTEXT_CACHE, "topic", "key",
                () -> (Producer<Object>) producer);
        cache.close();
    }

    @Test
    public void shouldTolerateRuntimeExceptionInFlush() {
        ProducerCache cache = new ProducerCache();
        Producer producer = mock(Producer.class);
        when(producer.flushAsync()).thenThrow(new RuntimeException("Some exception"));
        when(producer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        cache.getOrCreateProducer(ProducerCache.CacheArea.CONTEXT_CACHE, "topic", "key",
                () -> (Producer<Object>) producer);
        cache.close();
    }

}