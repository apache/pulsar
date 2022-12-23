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
package org.apache.pulsar.client.impl;


import static org.apache.pulsar.common.api.proto.CompressionType.LZ4;
import static org.apache.pulsar.common.api.proto.CompressionType.NONE;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.compaction.CompactionTest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RawBatchMessageContainerImplTest {
    CompressionType compressionType;
    MessageCrypto msgCrypto;
    CryptoKeyReader cryptoKeyReader;
    Map<String, EncryptionContext.EncryptionKey> encryptKeys;

    public void setEncryptionAndCompression(boolean encrypt, boolean compress) {
        if (compress) {
            compressionType = LZ4;
        } else {
            compressionType = NONE;
        }

        if (encrypt) {
            cryptoKeyReader = new CompactionTest.EncKeyReader();
            msgCrypto = new MessageCryptoBc("test", false);
            String key = "client-ecdsa.pem";
            EncryptionKeyInfo publicKeyInfo = cryptoKeyReader.getPublicKey(key, null);
            encryptKeys = Map.of(
                    key, new EncryptionContext.EncryptionKey(publicKeyInfo.getKey(), publicKeyInfo.getMetadata()));
        } else {
            msgCrypto = null;
            cryptoKeyReader = null;
            encryptKeys = null;
        }
    }

    public MessageImpl createMessage(String topic, String value, int entryId) {
        MessageMetadata metadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("test")
                .setSequenceId(entryId);

        MessageIdImpl id = new MessageIdImpl(0, entryId, -1);

        if (compressionType != null) {
            metadata.setCompression(compressionType);
        }
        Optional<EncryptionContext> encryptionContext = null;
        if(encryptKeys != null) {
            EncryptionContext tmp = new EncryptionContext();
            tmp.setKeys(encryptKeys);
            encryptionContext = Optional.of(tmp);
        } else {
            encryptionContext = Optional.empty();
        }
        ByteBuf payload = Unpooled.copiedBuffer(value.getBytes());
        return new MessageImpl(topic, id,metadata, payload, encryptionContext, null, Schema.STRING);
    }


    @BeforeMethod
    public void setup() throws Exception {
        setEncryptionAndCompression(false, false);
    }
    @Test
    public void testToByteBuf() throws IOException {
        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(2);
        String topic = "my-topic";
        container.add(createMessage(topic, "hi-1", 0), null);
        container.add(createMessage(topic, "hi-2", 1), null);
        ByteBuf buf = container.toByteBuf();


        int idSize = buf.readInt();
        ByteBuf idBuf = buf.readBytes(idSize);
        MessageIdData idData = new MessageIdData();
        idData.parseFrom(idBuf, idSize);
        Assert.assertEquals(idData.getLedgerId(), 0);
        Assert.assertEquals(idData.getEntryId(), 1);
        Assert.assertEquals(idData.getPartition(), -1);


        int metadataAndPayloadSize = buf.readInt();
        ByteBuf metadataAndPayload = buf.readBytes(metadataAndPayloadSize);
        MessageImpl singleMessageMetadataAndPayload = MessageImpl.deserialize(metadataAndPayload);
        MessageMetadata metadata = singleMessageMetadataAndPayload.getMessageBuilder();
        Assert.assertEquals(metadata.getNumMessagesInBatch(), 2);
        Assert.assertEquals(metadata.getHighestSequenceId(), 1);
        Assert.assertEquals(metadata.getCompression(), NONE);

        SingleMessageMetadata messageMetadata = new SingleMessageMetadata();
        ByteBuf payload1 = Commands.deSerializeSingleMessageInBatch(
                singleMessageMetadataAndPayload.getPayload(), messageMetadata, 0, 2);
        ByteBuf payload2 = Commands.deSerializeSingleMessageInBatch(
                singleMessageMetadataAndPayload.getPayload(), messageMetadata, 1, 2);

        Assert.assertEquals(payload1.toString(Charset.defaultCharset()), "hi-1");
        Assert.assertEquals(payload2.toString(Charset.defaultCharset()), "hi-2");
        payload1.release();
        payload2.release();
        singleMessageMetadataAndPayload.release();
        metadataAndPayload.release();
        buf.release();
    }

    @Test
    public void testToByteBufWithCompressionAndEncryption() throws IOException {
        setEncryptionAndCompression(true, true);

        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(2);
        container.setCryptoKeyReader(cryptoKeyReader);
        String topic = "my-topic";
        container.add(createMessage(topic, "hi-1", 0), null);
        container.add(createMessage(topic, "hi-2", 1), null);
        ByteBuf buf = container.toByteBuf();

        int idSize = buf.readInt();
        ByteBuf idBuf = buf.readBytes(idSize);
        MessageIdData idData = new MessageIdData();
        idData.parseFrom(idBuf, idSize);
        Assert.assertEquals(idData.getLedgerId(), 0);
        Assert.assertEquals(idData.getEntryId(), 1);
        Assert.assertEquals(idData.getPartition(), -1);

        int metadataAndPayloadSize = buf.readInt();
        ByteBuf metadataAndPayload = buf.readBytes(metadataAndPayloadSize);
        MessageImpl singleMessageMetadataAndPayload = MessageImpl.deserialize(metadataAndPayload);

        MessageMetadata metadata = singleMessageMetadataAndPayload.getMessageBuilder();
        Assert.assertEquals(metadata.getNumMessagesInBatch(), 2);
        Assert.assertEquals(metadata.getHighestSequenceId(), 1);
        Assert.assertEquals(metadata.getCompression(), compressionType);

        ByteBuf payload = singleMessageMetadataAndPayload.getPayload();
        int maxDecryptedSize = msgCrypto.getMaxOutputSize(payload.readableBytes());
        ByteBuffer decrypted = ByteBuffer.allocate(maxDecryptedSize);
        msgCrypto.decrypt(() -> metadata, payload.nioBuffer(), decrypted, cryptoKeyReader);
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        ByteBuf uncompressed = codec.decode(Unpooled.wrappedBuffer(decrypted),
                metadata.getUncompressedSize());
        SingleMessageMetadata messageMetadata = new SingleMessageMetadata();

        ByteBuf payload1 = Commands.deSerializeSingleMessageInBatch(
                uncompressed, messageMetadata, 0, 2);
        ByteBuf payload2 = Commands.deSerializeSingleMessageInBatch(
                uncompressed, messageMetadata, 1, 2);

        Assert.assertEquals(payload1.toString(Charset.defaultCharset()), "hi-1");
        Assert.assertEquals(payload2.toString(Charset.defaultCharset()), "hi-2");
        payload1.release();
        payload2.release();
        singleMessageMetadataAndPayload.release();
        metadataAndPayload.release();
        uncompressed.release();
        buf.release();
    }

    @Test
    public void testToByteBufWithSingleMessage() throws IOException {
        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(2);
        String topic = "my-topic";
        container.add(createMessage(topic, "hi-1", 0), null);
        ByteBuf buf = container.toByteBuf();


        int idSize = buf.readInt();
        ByteBuf idBuf = buf.readBytes(idSize);
        MessageIdData idData = new MessageIdData();
        idData.parseFrom(idBuf, idSize);
        Assert.assertEquals(idData.getLedgerId(), 0);
        Assert.assertEquals(idData.getEntryId(), 0);
        Assert.assertEquals(idData.getPartition(), -1);


        int metadataAndPayloadSize = buf.readInt();
        ByteBuf metadataAndPayload = buf.readBytes(metadataAndPayloadSize);
        MessageImpl singleMessageMetadataAndPayload = MessageImpl.deserialize(metadataAndPayload);
        MessageMetadata metadata = singleMessageMetadataAndPayload.getMessageBuilder();
        Assert.assertEquals(metadata.getNumMessagesInBatch(), 1);
        Assert.assertEquals(metadata.getHighestSequenceId(), 0);
        Assert.assertEquals(metadata.getCompression(), NONE);

        Assert.assertEquals(singleMessageMetadataAndPayload.getPayload().toString(Charset.defaultCharset()), "hi-1");
        singleMessageMetadataAndPayload.release();
        metadataAndPayload.release();
        buf.release();
    }

    @Test
    public void testMaxNumMessagesInBatch() {
        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(1);
        String topic = "my-topic";

        boolean isFull = container.add(createMessage(topic, "hi", 0), null);
        Assert.assertTrue(isFull);
        Assert.assertTrue(container.isBatchFull());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCreateOpSendMsg() {
        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(1);
        container.createOpSendMsg();
    }

    @Test
    public void testToByteBufWithEncryptionWithoutCryptoKeyReader() {
        setEncryptionAndCompression(true, false);
        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(1);
        String topic = "my-topic";
        container.add(createMessage(topic, "hi-1", 0), null);
        Assert.assertEquals(container.getNumMessagesInBatch(), 1);
        Throwable e = null;
        try {
            container.toByteBuf();
        } catch (IllegalStateException ex){
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalStateException.class);
        Assert.assertEquals(container.getNumMessagesInBatch(), 0);
        Assert.assertEquals(container.batchedMessageMetadataAndPayload, null);
    }

    @Test
    public void testToByteBufWithEncryptionWithInvalidEncryptKeys() {
        setEncryptionAndCompression(true, false);
        RawBatchMessageContainerImpl container = new RawBatchMessageContainerImpl(1);
        container.setCryptoKeyReader(cryptoKeyReader);
        encryptKeys = new HashMap<>();
        encryptKeys.put(null, null);
        String topic = "my-topic";
        container.add(createMessage(topic, "hi-1", 0), null);
        Assert.assertEquals(container.getNumMessagesInBatch(), 1);
        Throwable e = null;
        try {
            container.toByteBuf();
        } catch (IllegalArgumentException ex){
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
        Assert.assertEquals(container.getNumMessagesInBatch(), 0);
        Assert.assertEquals(container.batchedMessageMetadataAndPayload, null);
    }
}
