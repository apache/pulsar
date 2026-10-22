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
package org.apache.pulsar.broker.transaction.buffer.impl;

import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Default {@link TransactionBufferProvider}: returns {@link MetadataTransactionBuffer} for
 * {@code segment://} topics (PIP-473) and falls back to {@link TopicTransactionBuffer} for
 * {@code persistent://} / {@code topic://}. This is the configured default so segment topics
 * pick up the metadata-driven implementation out of the box without operators flipping a knob.
 */
public class DispatchingTransactionBufferProvider implements TransactionBufferProvider {

    private final TransactionBufferProvider legacy = new TopicTransactionBufferProvider();
    private final TransactionBufferProvider metadata = new MetadataTransactionBufferProvider();

    @Override
    public TransactionBuffer newTransactionBuffer(Topic originTopic) {
        return TopicName.get(originTopic.getName()).isSegment()
                ? metadata.newTransactionBuffer(originTopic)
                : legacy.newTransactionBuffer(originTopic);
    }
}
