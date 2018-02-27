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

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;

class TypedReaderConfigAdapter<T> extends ReaderConfigurationData<byte[]> {
    private final ReaderConfigurationData<T> typedConfig;
    private final Schema<T> codec;

    private TypedReaderImpl<T> typedReader;

    public TypedReaderConfigAdapter(ReaderConfigurationData<T> typedConfig, Schema<T> codec) {
        this.typedConfig = typedConfig;
        this.codec = codec;
    }

    void setTypedReader(TypedReaderImpl<T> typedReader) {
        this.typedReader = typedReader;
    }

    @Override
    public ReaderListener<byte[]> getReaderListener() {
        final ReaderListener<T> listener = typedConfig.getReaderListener();
        return new ReaderListener<byte[]>() {
            @Override
            public void received(Reader<byte[]> ignore, Message<byte[]> msg) {
                listener.received(typedReader, new TypedMessageImpl<>(msg, codec));
            }

            @Override
            public void reachedEndOfTopic(Reader<byte[]> ignore) {
                listener.reachedEndOfTopic(typedReader);
            }
        };
    }

    @Override
    public int getReceiverQueueSize() {
        return typedConfig.getReceiverQueueSize();
    }

    @Override
    public ConsumerCryptoFailureAction getCryptoFailureAction() {
        return typedConfig.getCryptoFailureAction();
    }

    @Override
    public CryptoKeyReader getCryptoKeyReader() {
        return typedConfig.getCryptoKeyReader();
    }

    @Override
    public String getReaderName() {
        return typedConfig.getReaderName();
    }
}
