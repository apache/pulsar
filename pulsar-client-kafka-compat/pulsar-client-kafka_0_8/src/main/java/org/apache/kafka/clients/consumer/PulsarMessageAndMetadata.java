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
package org.apache.kafka.clients.consumer;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

public class PulsarMessageAndMetadata<K, V> extends MessageAndMetadata<K, V> {

    private static final long serialVersionUID = 1L;
    private final String topic;
    private final int partition;
    private final long offset;
    private final K key;
    private final V value;
    private final Decoder<K> keyDecoder;
    private final Decoder<V> valueDecoder;

    public PulsarMessageAndMetadata(String topic, int partition, Message rawMessage, long offset, Decoder<K> keyDecoder,
            Decoder<V> valueDecoder, K key, V value) {
        super(topic, partition, rawMessage, offset, keyDecoder, valueDecoder);
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.key = key;
        this.value = value;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public V message() {
        return this.value;
    }

    @Override
    public K key() {
        return key;
    }

    @Override
    public Decoder<V> valueDecoder() {
        return this.valueDecoder;
    }

    @Override
    public Decoder<K> keyDecoder() {
        return this.keyDecoder;
    }

    @Override
    public Message rawMessage$1() {
        throw new UnsupportedOperationException();
    }
}
