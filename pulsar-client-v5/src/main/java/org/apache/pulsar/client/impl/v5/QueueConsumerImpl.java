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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.QueueConsumer;

/**
 * Internal extension of {@link QueueConsumer} that exposes the async hooks
 * needed by {@link AsyncQueueConsumerV5}. Implemented by both
 * {@link ScalableQueueConsumer} (single-topic) and {@link MultiTopicQueueConsumer}
 * so the async wrapper works against either without duplication.
 */
interface QueueConsumerImpl<T> extends QueueConsumer<T> {
    CompletableFuture<Message<T>> receiveAsync();

    CompletableFuture<Void> closeAsync();
}
