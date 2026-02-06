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
package org.apache.pulsar.websocket.service;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/***
 * This class is used in scenarios where the payload of the message has been encrypted and the producer does not need
 * to encrypt it again.
 * It discards payload encryption and only relies {@link #metadataModifierForSend} to set the encryption info into the
 * message metadata.
 */
public class WSSDummyMessageCryptoImpl implements MessageCrypto<MessageMetadata, MessageMetadata> {

    public static final WSSDummyMessageCryptoImpl INSTANCE_FOR_CONSUMER =
            new WSSDummyMessageCryptoImpl(msgMetadata -> {});

    private final Consumer<MessageMetadata> metadataModifierForSend;

    public WSSDummyMessageCryptoImpl(Consumer<MessageMetadata> metadataModifierForSend) {
        this.metadataModifierForSend = metadataModifierForSend;
    }

    @Override
    public void addPublicKeyCipher(Set keyNames, CryptoKeyReader keyReader)
            throws PulsarClientException.CryptoException {}

    @Override
    public boolean removeKeyCipher(String keyName) {
        return true;
    }

    @Override
    public int getMaxOutputSize(int inputLen) {
        return inputLen;
    }

    @Override
    public boolean decrypt(Supplier<MessageMetadata> messageMetadataSupplier, ByteBuffer payload, ByteBuffer outBuffer,
                           CryptoKeyReader keyReader) {
        outBuffer.put(payload);
        outBuffer.flip();
        return true;
    }

    @Override
    public synchronized void encrypt(Set<String> encKeys, CryptoKeyReader keyReader,
                                    Supplier<MessageMetadata> messageMetadataSupplier,
                                    ByteBuffer payload, ByteBuffer outBuffer) throws PulsarClientException {
        outBuffer.put(payload);
        outBuffer.flip();
        metadataModifierForSend.accept(messageMetadataSupplier.get());
    }
}
