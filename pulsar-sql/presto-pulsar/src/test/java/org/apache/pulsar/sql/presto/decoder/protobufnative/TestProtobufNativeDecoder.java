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
package org.apache.pulsar.sql.presto.decoder.protobufnative;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.decoder.AbstractDecoderTester;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.apache.pulsar.sql.presto.TestPulsarConnector.getPulsarConnectorId;
import static org.testng.Assert.assertTrue;

public class TestProtobufNativeDecoder extends AbstractDecoderTester {

    private ProtobufNativeSchema schema;

    @BeforeMethod
    public void init() {
        super.init();
        schema = ProtobufNativeSchema.of(TestMsg.TestMessage.class);
        schemaInfo = schema.getSchemaInfo();
        pulsarColumnHandle = getColumnColumnHandles(topicName, schemaInfo, PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        pulsarRowDecoder = decoderFactory.createRowDecoder(topicName, schemaInfo, new HashSet<>(pulsarColumnHandle));
        decoderTestUtil = new ProtobufNativeDecoderTestUtil();
        assertTrue(pulsarRowDecoder instanceof PulsarProtobufNativeRowDecoder);
    }

    @Test
    public void testPrimitiveType() {
        //Time: 2921-1-1
        long mills = 30010669261001L;
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(mills / 1000)
                .setNanos((int) (mills % 1000) * 1000000)
                .build();

        TestMsg.TestMessage testMessage = TestMsg.TestMessage.newBuilder()
                .setStringField("aaa")
                .setDoubleField(3.3D)
                .setFloatField(1.1f)
                .setInt32Field(33)
                .setInt64Field(44L)
                .setUint32Field(33)
                .setUint64Field(33L)
                .setSint32Field(12)
                .setSint64Field(13L)
                .setFixed32Field(22)
                .setFixed64Field(23L)
                .setSfixed32Field(31)
                .setSfixed64Field(32L)
                .setBoolField(true)
                .setBytesField(ByteString.copyFrom("abc".getBytes()))
                .setTestEnum(TestMsg.TestEnum.FAILOVER)
                .setTimestampField(timestamp)
                .build();

        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(schema.encode(testMessage));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();

        PulsarColumnHandle stringFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "stringField", VARCHAR, false, false, "stringField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, stringFieldColumnHandle, testMessage.getStringField());

        PulsarColumnHandle doubleFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "doubleField", DOUBLE, false, false, "doubleField", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, doubleFieldColumnHandle, testMessage.getDoubleField());

        PulsarColumnHandle int32FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "int32Field", INTEGER, false, false, "int32Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, int32FieldColumnHandle, testMessage.getInt32Field());

        PulsarColumnHandle int64FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "int64Field", BIGINT, false, false, "int64Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, int64FieldColumnHandle, testMessage.getInt64Field());

        PulsarColumnHandle uint32FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "uint32Field", INTEGER, false, false, "uint32Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, uint32FieldColumnHandle, testMessage.getUint32Field());

        PulsarColumnHandle uint64FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "uint64Field", BIGINT, false, false, "uint64Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, uint64FieldColumnHandle, testMessage.getUint64Field());

        PulsarColumnHandle sint32FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "sint32Field", INTEGER, false, false, "sint32Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, sint32FieldColumnHandle, testMessage.getSint32Field());

        PulsarColumnHandle sint64FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "sint64Field", BIGINT, false, false, "sint64Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, sint64FieldColumnHandle, testMessage.getSint64Field());

        PulsarColumnHandle fixed32FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "fixed32Field", INTEGER, false, false, "fixed32Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, fixed32FieldColumnHandle, testMessage.getFixed32Field());

        PulsarColumnHandle fixed64FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "fixed64Field", BIGINT, false, false, "fixed64Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, fixed64FieldColumnHandle, testMessage.getFixed64Field());

        PulsarColumnHandle sfixed32FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "sfixed32Field", INTEGER, false, false, "sfixed32Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, sfixed32FieldColumnHandle, testMessage.getSfixed32Field());

        PulsarColumnHandle sfixed64FieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "sfixed64Field", BIGINT, false, false, "sfixed64Field", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, sfixed64FieldColumnHandle, testMessage.getSfixed64Field());

        PulsarColumnHandle boolFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "boolField", BOOLEAN, false, false, "boolField", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, boolFieldColumnHandle, testMessage.getBoolField());

        PulsarColumnHandle bytesFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "bytesField", VARBINARY, false, false, "bytesField", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, bytesFieldColumnHandle, testMessage.getBytesField().toStringUtf8());

        PulsarColumnHandle enumFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "testEnum", VARCHAR, false, false, "testEnum", null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, enumFieldColumnHandle, testMessage.getTestEnum().name());

        PulsarColumnHandle timestampFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "timestampField", TIMESTAMP,false,false,"timestampField",null,null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, timestampFieldColumnHandle, mills);

    }

    @Test
    public void testRow() {

        TestMsg.SubMessage.NestedMessage nestedMessage = TestMsg.SubMessage.NestedMessage.newBuilder()
                .setTitle("nestedMessage_title")
                .addUrls("aa")
                .addUrls("bb")
                .build();
        TestMsg.SubMessage subMessage = TestMsg.SubMessage.newBuilder()
                .setBar(0.2)
                .setFoo("fooValue")
                .setBar(3.9d)
                .setNestedMessage(nestedMessage)
                .build();

        TestMsg.TestMessage testMessage = TestMsg.TestMessage.newBuilder().setSubMessage(subMessage).build();

        byte[] bytes = schema.encode(testMessage);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);

        GenericProtobufNativeRecord genericRecord =
                (GenericProtobufNativeRecord) GenericProtobufNativeSchema.of(schemaInfo).decode(bytes);
        Object fieldValue =
                genericRecord.getProtobufRecord().getField(genericRecord.getProtobufRecord().getDescriptorForType().findFieldByName("subMessage"));

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();
        RowType columnType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("foo", VARCHAR))
                .add(RowType.field("bar", DOUBLE))
                .add(RowType.field("nestedMessage", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("title", VARCHAR))
                        .add(RowType.field("urls", new ArrayType(VARCHAR)))
                        .build())))
                .build());

        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "subMessage", columnType, false, false, "subMessage", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);

        checkRowValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);

    }

    @Test
    public void testArray() {

        TestMsg.TestMessage testMessage = TestMsg.TestMessage.newBuilder()
                .addRepeatedField("first").addRepeatedField("second")
                .build();

        byte[] bytes = schema.encode(testMessage);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);

        GenericProtobufNativeRecord genericRecord =
                (GenericProtobufNativeRecord) GenericProtobufNativeSchema.of(schemaInfo).decode(bytes);
        Object fieldValue =
                genericRecord.getProtobufRecord().getField(genericRecord.getProtobufRecord().getDescriptorForType().findFieldByName("repeatedField"));

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();

        ArrayType columnType = new ArrayType(VARCHAR);
        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "repeatedField", columnType, false, false, "repeatedField",
                null, null, PulsarColumnHandle.HandleKeyValueType.NONE);

        checkArrayValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);
    }


    @Test
    public void testMap() {

        TestMsg.TestMessage testMessage = TestMsg.TestMessage.newBuilder()
                .putMapField("key_a", 1.1d)
                .putMapField("key_b", 2.2d)
                .build();

        byte[] bytes = schema.encode(testMessage);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);

        GenericProtobufNativeRecord genericRecord =
                (GenericProtobufNativeRecord) GenericProtobufNativeSchema.of(schemaInfo).decode(bytes);
        Object fieldValue =
                genericRecord.getProtobufRecord().getField(genericRecord.getProtobufRecord().getDescriptorForType().findFieldByName("mapField"));

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();

        Type columnType = decoderFactory.getTypeManager().getParameterizedType(StandardTypes.MAP,
                ImmutableList.of(TypeSignatureParameter.typeParameter(VARCHAR.getTypeSignature()),
                        TypeSignatureParameter.typeParameter(DOUBLE.getTypeSignature())));

        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(), "mapField", columnType, false, false,
                "mapField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkMapValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);

    }

}
