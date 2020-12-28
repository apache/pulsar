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
package org.apache.pulsar.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.testng.Assert;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CommandUtilsTests {

    @Test
    public void testToKeyValueList() {
        List<PulsarApi.KeyValue> keyValues = CommandUtils.toKeyValueList(null);
        Assert.assertNotNull(keyValues);
        Assert.assertTrue(keyValues.isEmpty());

        final Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");

        keyValues = CommandUtils.toKeyValueList(metadata);
        Assert.assertEquals(keyValues.size(), keyValues.size());
        PulsarApi.KeyValue kv = keyValues.get(0);
        final Map.Entry<String, String> entry = metadata.entrySet().iterator().next();
        Assert.assertEquals(kv.getKey(), entry.getKey());
        Assert.assertEquals(kv.getValue(), entry.getValue());
    }

    @Test
    public void testMetadataFromCommandProducer() {
        Map<String, String> metadata = CommandUtils.metadataFromCommand(newCommandProducer(null, null));
        Assert.assertNotNull(metadata);
        Assert.assertTrue(metadata.isEmpty());

        final String key = "key";
        final String value = "value";

        PulsarApi.CommandProducer cmd = newCommandProducer(key, value);
        metadata = CommandUtils.metadataFromCommand(cmd);
        Assert.assertEquals(1, metadata.size());
        final Map.Entry<String, String> entry = metadata.entrySet().iterator().next();
        Assert.assertEquals(key, entry.getKey());
        Assert.assertEquals(value, entry.getValue());
    }

    @Test
    public void testMetadataFromCommandSubscribe() {
        Map<String, String> metadata = CommandUtils.metadataFromCommand(newCommandSubscribe(null, null));
        Assert.assertNotNull(metadata);
        Assert.assertTrue(metadata.isEmpty());

        final String key = "key";
        final String value = "value";

        PulsarApi.CommandSubscribe cmd = newCommandSubscribe(key, value);
        metadata = CommandUtils.metadataFromCommand(cmd);
        Assert.assertEquals(1, metadata.size());
        final Map.Entry<String, String> entry = metadata.entrySet().iterator().next();
        Assert.assertEquals(key, entry.getKey());
        Assert.assertEquals(value, entry.getValue());
    }

    private PulsarApi.CommandProducer newCommandProducer(String key, String value) {
        PulsarApi.CommandProducer.Builder cmd = PulsarApi.CommandProducer.newBuilder()
                .setProducerId(1)
                .setRequestId(1)
                .setTopic("my-topic")
                .setProducerName("producer");

        if (key != null && value != null) {
            cmd.addMetadata(PulsarApi.KeyValue.newBuilder().setKey(key).setValue(value).build());
        }

        return cmd.build();
    }

    private PulsarApi.CommandSubscribe newCommandSubscribe(String key, String value) {
        PulsarApi.CommandSubscribe.Builder cmd = PulsarApi.CommandSubscribe.newBuilder()
                .setConsumerId(1)
                .setRequestId(1)
                .setTopic("my-topic")
                .setSubscription("my-subscription")
                .setSubType(PulsarApi.CommandSubscribe.SubType.Shared);

        if (key != null && value != null) {
            cmd.addMetadata(PulsarApi.KeyValue.newBuilder().setKey(key).setValue(value).build());
        }

        return cmd.build();
    }

    @Test
    public void testByteBufComposite() throws Exception {
        String HEAD = "head-";
        String TAIL = "tail";
        ByteBuf b1 = PulsarByteBufAllocator.DEFAULT.buffer();
        b1.writeBytes(HEAD.getBytes(StandardCharsets.UTF_8));

        ByteBuf b2 = PulsarByteBufAllocator.DEFAULT.buffer();
        b2.writeBytes(TAIL.getBytes(StandardCharsets.UTF_8));

        CompositeByteBuf b3 = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        b3.addComponents(true, b1, b2);

        assertEquals(0, b3.readerIndex());
        assertEquals(b1.readableBytes() + b2.readableBytes(), b3.writerIndex());
        assertEquals(b1.readableBytes() + b2.readableBytes(), b3.readableBytes());

        byte[] content = new byte[b1.readableBytes() + b2.readableBytes()];
        b3.readBytes(content);
        assertEquals(HEAD + TAIL, new String(content, StandardCharsets.UTF_8));
    }

    @Test
    public void testAddBrokerEntryMetadata() throws Exception {
        int MOCK_BATCH_SIZE = 10;
        String data = "test-message";
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
        byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));

        PulsarApi.BrokerEntryMetadata brokerMetadata =
                PulsarApi.BrokerEntryMetadata
                        .newBuilder()
                        .setBrokerTimestamp(System.currentTimeMillis())
                        .setIndex(MOCK_BATCH_SIZE - 1)
                        .build();
        ByteBuf dataWithBrokerEntryMetadata =
                Commands.addBrokerEntryMetadata(byteBuf, getBrokerEntryMetadataInterceptors(), MOCK_BATCH_SIZE);
        assertEquals(brokerMetadata.getSerializedSize() + data.length() + 6,
                dataWithBrokerEntryMetadata.readableBytes());

        byte [] content = new byte[dataWithBrokerEntryMetadata.readableBytes()];
        dataWithBrokerEntryMetadata.readBytes(content);
        assertTrue(new String(content, StandardCharsets.UTF_8).endsWith(data));
    }

    @Test
    public void testSkipBrokerEntryMetadata() throws Exception {
        String data = "test-message";
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
        byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));
        ByteBuf dataWithBrokerEntryMetadata =
                Commands.addBrokerEntryMetadata(byteBuf, getBrokerEntryMetadataInterceptors(), 11);

        Commands.skipBrokerEntryMetadataIfExist(dataWithBrokerEntryMetadata);
        assertEquals(data.length(), dataWithBrokerEntryMetadata.readableBytes());

        byte [] content = new byte[dataWithBrokerEntryMetadata.readableBytes()];
        dataWithBrokerEntryMetadata.readBytes(content);
        assertEquals(new String(content, StandardCharsets.UTF_8), data);
    }

    @Test
    public void testParseBrokerEntryMetadata() throws Exception {
        int MOCK_BATCH_SIZE = 10;
        String data = "test-message";
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
        byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));
        ByteBuf dataWithBrokerEntryMetadata =
                Commands.addBrokerEntryMetadata(byteBuf, getBrokerEntryMetadataInterceptors(), MOCK_BATCH_SIZE);
        PulsarApi.BrokerEntryMetadata brokerMetadata =
                Commands.parseBrokerEntryMetadataIfExist(dataWithBrokerEntryMetadata);

        assertTrue(brokerMetadata.getBrokerTimestamp() <= System.currentTimeMillis());
        assertEquals(brokerMetadata.getIndex(), MOCK_BATCH_SIZE - 1);
        assertEquals(data.length(), dataWithBrokerEntryMetadata.readableBytes());

        byte [] content = new byte[dataWithBrokerEntryMetadata.readableBytes()];
        dataWithBrokerEntryMetadata.readBytes(content);
        assertEquals(new String(content, StandardCharsets.UTF_8), data);
    }

    @Test
    public void testPeekBrokerEntryMetadata() throws Exception {
        int MOCK_BATCH_SIZE = 10;
        String data = "test-message";
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
        byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));
        ByteBuf dataWithBrokerEntryMetadata =
                Commands.addBrokerEntryMetadata(byteBuf, getBrokerEntryMetadataInterceptors(), MOCK_BATCH_SIZE);
        int bytesBeforePeek = dataWithBrokerEntryMetadata.readableBytes();
        PulsarApi.BrokerEntryMetadata brokerMetadata =
                Commands.peekBrokerEntryMetadataIfExist(dataWithBrokerEntryMetadata);

        assertTrue(brokerMetadata.getBrokerTimestamp() <= System.currentTimeMillis());
        assertEquals(brokerMetadata.getIndex(), MOCK_BATCH_SIZE - 1);

        int bytesAfterPeek = dataWithBrokerEntryMetadata.readableBytes();
        assertEquals(bytesBeforePeek, bytesAfterPeek);

        // test parse logic after peek

        PulsarApi.BrokerEntryMetadata brokerMetadata1 =
                Commands.parseBrokerEntryMetadataIfExist(dataWithBrokerEntryMetadata);
        assertTrue(brokerMetadata1.getBrokerTimestamp() <= System.currentTimeMillis());

        assertEquals(brokerMetadata1.getIndex(), MOCK_BATCH_SIZE - 1);
        assertEquals(data.length(), dataWithBrokerEntryMetadata.readableBytes());

        byte [] content = new byte[dataWithBrokerEntryMetadata.readableBytes()];
        dataWithBrokerEntryMetadata.readBytes(content);
        assertEquals(new String(content, StandardCharsets.UTF_8), data);
    }

    public Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }


    public ByteBuf getMessage(String producerName, long seqId) {
        PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                .setProducerName(producerName).setSequenceId(seqId)
                .setPublishTime(System.currentTimeMillis()).build();

        return serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, messageMetadata, io.netty.buffer.Unpooled.copiedBuffer(new byte[0]));
    }
}
