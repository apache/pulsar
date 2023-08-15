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
package org.apache.pulsar.websocket.proxy;

import static org.apache.pulsar.client.impl.crypto.MessageCryptoBc.ECDSA;
import static org.apache.pulsar.client.impl.crypto.MessageCryptoBc.ECIES;
import static org.apache.pulsar.client.impl.crypto.MessageCryptoBc.RSA;
import static org.apache.pulsar.client.impl.crypto.MessageCryptoBc.RSA_TRANS;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.spec.AlgorithmParameterSpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

@Slf4j
public class WssClientSideEncryptUtils {

    public static Charset UTF8 = StandardCharsets.UTF_8;

    public static String base64AndUrlEncode(String str) {
        return base64AndUrlEncode(str.getBytes(UTF8), UTF8);
    }

    public static String base64AndUrlEncode(String str, Charset charset) {
        return base64AndUrlEncode(str.getBytes(charset), charset);
    }

    public static String base64Encode(String str, Charset charset) {
        return Base64.getEncoder().encodeToString(str.getBytes(charset));
    }

    public static String base64Encode(String str) {
        return Base64.getEncoder().encodeToString(str.getBytes(UTF8));
    }

    public static byte[] base64Decode(String str) {
        return Base64.getDecoder().decode(str);
    }

    public static String base64Encode(byte[] byteArray) {
        return Base64.getEncoder().encodeToString(byteArray);
    }

    public static String base64AndUrlEncode(byte[] byteArray) {
        String base64Encode = Base64.getEncoder().encodeToString(byteArray);
        return URLEncoder.encode(base64Encode, UTF8);
    }

    public static String base64AndUrlEncode(byte[] byteArray, Charset charset) {
        String base64Encode = Base64.getEncoder().encodeToString(byteArray);
        return URLEncoder.encode(base64Encode, charset);
    }

    public static String urlEncode(String str) {
        return URLEncoder.encode(str, UTF8);
    }

    public static String urlEncode(String str, Charset charset) {
        return URLEncoder.encode(str, charset);
    }

    public static byte[] calculateEncryptedKey(MessageCryptoBc msgCrypto, CryptoKeyReader cryptoKeyReader,
                                               String publicKeyName)
            throws PulsarClientException.CryptoException {
        EncryptionKeyInfo encryptionKeyInfo = cryptoKeyReader.getPublicKey(publicKeyName, Collections.emptyMap());
        return calculateEncryptedKey(msgCrypto, encryptionKeyInfo.getKey());
    }

    public static String base64EncodePublicKeyDataMetadata(MessageCryptoBc msgCrypto, CryptoKeyReader cryptoKeyReader,
                                               String publicKeyName)
            throws PulsarClientException.CryptoException {
        try {
            EncryptionKeyInfo encryptionKeyInfo = cryptoKeyReader.getPublicKey(publicKeyName, Collections.emptyMap());
            final List<KeyValue> entryList = new ArrayList<>();
            if (encryptionKeyInfo.getMetadata() != null) {
                for (Map.Entry<String, String> entry : encryptionKeyInfo.getMetadata().entrySet()) {
                    entryList.add(new KeyValue().setKey(entry.getKey()).setValue(entry.getValue()));
                }
            }
            String json = ObjectMapperFactory.getMapper().getObjectMapper()
                    .writeValueAsString(entryList);
            return base64Encode(json);
        } catch (JsonProcessingException e) {
            throw new PulsarClientException.CryptoException("Serialize encryption public key metadata failed");
        }
    }

    public static byte[] calculateEncryptedKey(MessageCryptoBc msgCrypto, EncryptionKeyInfo encryptionKeyInfo)
            throws Exception {
        return calculateEncryptedKey(msgCrypto, encryptionKeyInfo.getKey());
    }

    public static byte[] calculateEncryptedKey(MessageCryptoBc msgCrypto, byte[] publicKeyData)
            throws PulsarClientException.CryptoException {
        try {
            PublicKey pubKey = MessageCryptoBc.loadPublicKey(publicKeyData);
            Cipher dataKeyCipher = loadAndInitCipher(pubKey);
            return dataKeyCipher.doFinal(msgCrypto.getDataKey().getEncoded());
        } catch (Exception e) {
            log.error("Failed to encrypt data key. {}", e.getMessage());
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    private static Cipher loadAndInitCipher(PublicKey pubKey) throws PulsarClientException.CryptoException,
            NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, InvalidKeyException,
            InvalidAlgorithmParameterException {
        Cipher dataKeyCipher = null;
        AlgorithmParameterSpec params = null;
        // Encrypt data key using public key
        if (RSA.equals(pubKey.getAlgorithm())) {
            dataKeyCipher = Cipher.getInstance(RSA_TRANS, BouncyCastleProvider.PROVIDER_NAME);
        } else if (ECDSA.equals(pubKey.getAlgorithm())) {
            dataKeyCipher = Cipher.getInstance(ECIES, BouncyCastleProvider.PROVIDER_NAME);
            params = MessageCryptoBc.createIESParameterSpec();
        } else {
            String msg =  "Unsupported key type " + pubKey.getAlgorithm();
            log.error(msg);
            throw new PulsarClientException.CryptoException(msg);
        }
        if (params != null) {
            dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey, params);
        } else {
            dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey);
        }
        return dataKeyCipher;
    }

    public static byte[] compressionIfNeeded(CompressionType compressionType, byte[] payload) {
        if (compressionType != null && !CompressionType.NONE.equals(compressionType)) {
            CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
            ByteBuf input = PulsarByteBufAllocator.DEFAULT.buffer(payload.length, payload.length);
            input.writeBytes(payload);
            ByteBuf output = codec.encode(input);
            input.release();
            byte[] res = new byte[output.readableBytes()];
            output.readBytes(res);
            output.release();
            return res;
        }
        return payload;
    }

    public static EncryptedPayloadAndParam encryptPayload(CryptoKeyReader cryptoKeyReader, MessageCryptoBc msgCrypto,
                                                           byte[] payload, String keyName)
            throws PulsarClientException {
        ByteBuffer unEncryptedMessagePayload = ByteBuffer.wrap(payload);
        ByteBuffer encryptedMessagePayload = ByteBuffer.allocate(unEncryptedMessagePayload.remaining() + 512);
        MessageMetadata ignoredMessageMetadata = new MessageMetadata();
        msgCrypto.encrypt(Collections.singleton(keyName), cryptoKeyReader,
                () -> ignoredMessageMetadata, unEncryptedMessagePayload, encryptedMessagePayload);
        byte[] res = new byte[encryptedMessagePayload.remaining()];
        encryptedMessagePayload.get(res);
        return new EncryptedPayloadAndParam(WssClientSideEncryptUtils.base64Encode(res),
                WssClientSideEncryptUtils.base64Encode(ignoredMessageMetadata.getEncryptionParam()));
    }

    @AllArgsConstructor
    public static class EncryptedPayloadAndParam {
        public final String encryptedPayload;
        public final String encryptionParam;
    }

    private static final FastThreadLocal<SingleMessageMetadata> LOCAL_SINGLE_MESSAGE_METADATA =
            new FastThreadLocal<>() {
                @Override
                protected SingleMessageMetadata initialValue() throws Exception {
                    return new SingleMessageMetadata();
                }
            };

    public static byte[] compositeBatchMessage(List<ProducerMessage> messages) {
        ByteBuf batchBuffer = PulsarByteBufAllocator.DEFAULT.buffer(32, Commands.DEFAULT_MAX_MESSAGE_SIZE);
        for (ProducerMessage msg : messages) {
            // Param check.
            if (msg.payload == null) {
                throw new IllegalArgumentException("Null value message is not supported.");
            }

            // Get thread local SingleMessageMetadata.
            SingleMessageMetadata smm = LOCAL_SINGLE_MESSAGE_METADATA.get();
            smm.clear();

            // Build single message metadata.
            if (StringUtils.isNoneBlank(msg.getKey())) {
                smm.setPartitionKey(msg.getKey());
                smm.setPartitionKeyB64Encoded(false);
            }
            if (msg.getProperties() != null && !msg.getProperties().isEmpty()) {
                for (Map.Entry<String, String> prop : msg.getProperties().entrySet()) {
                    smm.addProperty()
                            .setKey(prop.getKey())
                            .setValue(prop.getValue());
                }
            }
            if (StringUtils.isNumeric(msg.getEventTime())) {
                smm.setEventTime(Long.valueOf(msg.getEventTime()));
            }
            smm.setSequenceId(msg.getSequenceId());
            // Null value is not supported now.
            // TODO support null value in the future.
            smm.setNullValue(false);
            smm.setNullPartitionKey(false);
            byte[] singleMsgPayload = msg.payload.getBytes(UTF8);
            smm.setPayloadSize(singleMsgPayload.length);

            // Append single message into batch message payload.
            batchBuffer.writeInt(smm.getSerializedSize());
            smm.writeTo(batchBuffer);
            batchBuffer.writeBytes(singleMsgPayload);
        }
        byte[] res = new byte[batchBuffer.readableBytes()];
        batchBuffer.readBytes(res);
        batchBuffer.release();
        return res;
    }

    public static byte[] decryptMsgPayload(String payloadString, EncryptionContext encryptionContext,
                                           CryptoKeyReader cryptoKeyReader, MessageCryptoBc msgCrypto) {
        byte[] payload = base64Decode(payloadString);
        if (encryptionContext == null) {
            return payload;
        }

        MessageMetadata messageMetadata = new MessageMetadata();
        Map<String, EncryptionContext.EncryptionKey> encKeys = encryptionContext.getKeys();
        for (Map.Entry<String, EncryptionContext.EncryptionKey> entry : encKeys.entrySet()) {
            EncryptionKeys encryptionKeys = messageMetadata.addEncryptionKey()
                    .setKey(entry.getKey()).setValue(entry.getValue().getKeyValue());
            if (entry.getValue().getMetadata() != null) {
                for (Map.Entry<String, String> prop : entry.getValue().getMetadata().entrySet()) {
                    encryptionKeys.addMetadata().setKey(prop.getKey()).setValue(prop.getValue());
                }
            }
        }
        messageMetadata.setEncryptionParam(encryptionContext.getParam());

        // Create input and output.
        ByteBuffer input = ByteBuffer.allocate(payload.length);
        ByteBuffer output = ByteBuffer.allocate(msgCrypto.getMaxOutputSize(payload.length));
        input.put(payload);
        input.flip();

        // Decrypt.
        msgCrypto.decrypt(() -> messageMetadata, input, output, cryptoKeyReader);
        byte[] res = new byte[output.limit()];
        output.get(res);
        return res;
    }

    public static byte[] unCompressionIfNeeded(byte[] payloadBytes, EncryptionContext encryptionContext) throws IOException {
        if (encryptionContext.getCompressionType() != null && !org.apache.pulsar.client.api.CompressionType.NONE
                .equals(encryptionContext.getCompressionType())) {
            CompressionCodec codec =
                    CompressionCodecProvider.getCompressionCodec(encryptionContext.getCompressionType());
            ByteBuf input = PulsarByteBufAllocator.DEFAULT.buffer(payloadBytes.length, payloadBytes.length);
            input.writeBytes(payloadBytes);
            ByteBuf output = codec.decode(input, encryptionContext.getUncompressedMessageSize());
            input.release();
            byte[] res = new byte[output.readableBytes()];
            output.readBytes(res);
            output.release();
            return res;
        }
        return payloadBytes;
    }

    /**
     * Note: this method does not parse the message in its entirety; it only parses the payload of the message.
     */
    public static List<ConsumerMessage> extractBatchMessagesIfNeeded(byte[] payloadBytes,
                                                              EncryptionContext encryptionContext) throws IOException {
        ByteBuf payload = PulsarByteBufAllocator.DEFAULT.buffer(payloadBytes.length);
        payload.writeBytes(payloadBytes);
        if (encryptionContext.getBatchSize().isPresent()) {
            List<ConsumerMessage> res = new ArrayList<>();
            int batchSize = encryptionContext.getBatchSize().get();
            for (int i = 0; i < batchSize; i++) {
                ConsumerMessage msg = new ConsumerMessage();
                SingleMessageMetadata singleMsgMetadata = new SingleMessageMetadata();
                ByteBuf singleMsgPayload = Commands.deSerializeSingleMessageInBatch(payload, singleMsgMetadata, i,
                        batchSize);
                if (singleMsgMetadata.getPayloadSize() < 1) {
                    msg.payload = null;
                } else {
                    byte[] bs = new byte[singleMsgPayload.readableBytes()];
                    singleMsgPayload.readBytes(bs);
                    msg.payload = new String(bs, UTF8);
                }
                res.add(msg);
            }
            return res;
        }
        ConsumerMessage msg = new ConsumerMessage();
        msg.payload = new String(payloadBytes, UTF8);
        return Collections.singletonList(msg);
    }
}
