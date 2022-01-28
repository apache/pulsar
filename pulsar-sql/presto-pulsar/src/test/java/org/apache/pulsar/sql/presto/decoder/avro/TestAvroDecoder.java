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
package org.apache.pulsar.sql.presto.decoder.avro;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.decoder.AbstractDecoderTester;
import org.apache.pulsar.sql.presto.decoder.DecoderTestMessage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static org.apache.pulsar.sql.presto.TestPulsarConnector.getPulsarConnectorId;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestAvroDecoder extends AbstractDecoderTester {

    private AvroSchema schema;

    @BeforeMethod
    public void init() {
        super.init();
        schema = AvroSchema.of(DecoderTestMessage.class);
        schemaInfo = schema.getSchemaInfo();
        pulsarColumnHandle = getColumnColumnHandles(topicName, schemaInfo, PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        pulsarRowDecoder = decoderFactory.createRowDecoder(topicName, schemaInfo, new HashSet<>(pulsarColumnHandle));
        decoderTestUtil = new AvroDecoderTestUtil();
        assertTrue(pulsarRowDecoder instanceof PulsarAvroRowDecoder);
    }

    @Test
    public void testPrimitiveType() {
        DecoderTestMessage message = new DecoderTestMessage();
        message.stringField = "message_1";
        message.intField = 22;
        message.floatField = 2.2f;
        message.doubleField = 22.20D;
        message.booleanField = true;
        message.longField = 222L;
        message.timestampField = System.currentTimeMillis();
        message.enumField = DecoderTestMessage.TestEnum.TEST_ENUM_1;

        LocalTime now = LocalTime.now(ZoneId.systemDefault());
        message.timeField = now.toSecondOfDay() * 1000;

        LocalDate localDate = LocalDate.now();
        LocalDate epoch = LocalDate.ofEpochDay(0);
        message.dateField = Math.toIntExact(ChronoUnit.DAYS.between(epoch, localDate));

        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(schema.encode(message));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();

        PulsarColumnHandle stringFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "stringField", VARCHAR, false, false, "stringField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, stringFieldColumnHandle, message.stringField);

        PulsarColumnHandle intFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "intField", INTEGER, false, false, "intField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, intFieldColumnHandle, message.intField);

        PulsarColumnHandle floatFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "floatField", REAL, false, false, "floatField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, floatFieldColumnHandle, floatToIntBits(message.floatField));

        PulsarColumnHandle doubleFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "doubleField", DOUBLE, false, false, "doubleField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, doubleFieldColumnHandle, message.doubleField);

        PulsarColumnHandle booleanFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "booleanField", BOOLEAN, false, false, "booleanField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, booleanFieldColumnHandle, message.booleanField);

        PulsarColumnHandle longFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "longField", BIGINT, false, false, "longField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, longFieldColumnHandle, message.longField);

        PulsarColumnHandle enumFieldColumnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "enumField", VARCHAR, false, false, "enumField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkValue(decodedRow, enumFieldColumnHandle, message.enumField.toString());

    }

    @Test
    public void testRow() {
        DecoderTestMessage message = new DecoderTestMessage();
        message.stringField = "message_2";
        DecoderTestMessage.TestRow testRow = new DecoderTestMessage.TestRow();
        message.rowField = testRow;
        testRow.intField = 22;
        testRow.stringField = "message_2_testRow";
        DecoderTestMessage.NestedRow nestedRow = new DecoderTestMessage.NestedRow();
        nestedRow.longField = 222L;
        nestedRow.stringField = "message_2_nestedRow";
        testRow.nestedRow = nestedRow;

        byte[] bytes = schema.encode(message);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);
        GenericAvroRecord genericRecord = (GenericAvroRecord) GenericAvroSchema.of(schemaInfo).decode(bytes);
        Object fieldValue = genericRecord.getAvroRecord().get("rowField");

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();
        RowType columnType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("intField", INTEGER))
                .add(RowType.field("nestedRow", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("longField", BIGINT))
                        .add(RowType.field("stringField", VARCHAR))
                        .build())))
                .add(RowType.field("stringField", VARCHAR))
                .build());

        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "rowField", columnType, false, false, "rowField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);

        checkRowValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);
    }

    @Test
    public void testArray() {
        DecoderTestMessage message = new DecoderTestMessage();
        message.arrayField = Arrays.asList("message_1", "message_2", "message_3");

        byte[] bytes = schema.encode(message);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);
        GenericAvroRecord genericRecord = (GenericAvroRecord) GenericAvroSchema.of(schemaInfo).decode(bytes);
        Object fieldValue = genericRecord.getAvroRecord().get("arrayField");
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();

        ArrayType columnType = new ArrayType(VARCHAR);
        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "arrayField", columnType, false, false, "arrayField",
                null, null, PulsarColumnHandle.HandleKeyValueType.NONE);

        checkArrayValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);
    }

    @Test
    public void testMap() {

        DecoderTestMessage message = new DecoderTestMessage();
        message.mapField = new HashMap<String, Long>() {{
            put("key1", 2L);
            put("key2", 22L);
        }};

        byte[] bytes = schema.encode(message);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);
        GenericAvroRecord genericRecord = (GenericAvroRecord) GenericAvroSchema.of(schemaInfo).decode(bytes);
        Object fieldValue = genericRecord.getAvroRecord().get("mapField");

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();
        Type columnType = decoderFactory.getTypeManager().getParameterizedType(StandardTypes.MAP,
                ImmutableList.of(TypeSignatureParameter.typeParameter(VarcharType.VARCHAR.getTypeSignature()),
                        TypeSignatureParameter.typeParameter(BigintType.BIGINT.getTypeSignature())));

        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(), "mapField", columnType, false, false,
                "mapField", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
        checkMapValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);
    }

    @Test
    public void testCompositeType() {
        DecoderTestMessage message = new DecoderTestMessage();

        DecoderTestMessage.NestedRow nestedRow = new DecoderTestMessage.NestedRow();
        nestedRow.longField = 222L;
        nestedRow.stringField = "message_2_nestedRow";

        DecoderTestMessage.CompositeRow compositeRow = new DecoderTestMessage.CompositeRow();
        DecoderTestMessage.NestedRow nestedRow1 = new DecoderTestMessage.NestedRow();
        nestedRow1.longField = 2;
        nestedRow1.stringField = "nestedRow_1";
        DecoderTestMessage.NestedRow nestedRow2 = new DecoderTestMessage.NestedRow();
        nestedRow2.longField = 2;
        nestedRow2.stringField = "nestedRow_2";
        compositeRow.arrayField = Arrays.asList(nestedRow1, nestedRow2);
        compositeRow.stringField = "compositeRow_1";

        compositeRow.mapField = new HashMap<String, DecoderTestMessage.NestedRow>() {{
            put("key1", nestedRow1);
            put("key2", nestedRow2);
        }};
        compositeRow.nestedRow = nestedRow;

        new HashMap<String, Long>() {{
            put("key1_1", 2L);
            put("key1_2", 22L);
        }};
        compositeRow.structedField = new HashMap<String, List<Long>>() {{
            put("key2_1", Arrays.asList(2L, 3L));
            put("key2_2", Arrays.asList(2L, 3L));
            put("key2_3", Arrays.asList(2L, 3L));
        }};


        message.compositeRow = compositeRow;
        byte[] bytes = schema.encode(message);
        ByteBuf payload = io.netty.buffer.Unpooled
                .copiedBuffer(bytes);
        GenericAvroRecord genericRecord = (GenericAvroRecord) GenericAvroSchema.of(schemaInfo).decode(bytes);
        Object fieldValue = genericRecord.getAvroRecord().get("compositeRow");

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = pulsarRowDecoder.decodeRow(payload).get();

        RowType columnType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("arrayField", new ArrayType(
                        RowType.from(ImmutableList.<RowType.Field>builder()
                                .add(RowType.field("longField", BIGINT))
                                .add(RowType.field("stringField", VARCHAR))
                                .build()))))
                .add(RowType.field("mapField", decoderFactory.getTypeManager().getParameterizedType(StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.typeParameter(VarcharType.VARCHAR.getTypeSignature()),
                                TypeSignatureParameter.typeParameter(RowType.from(ImmutableList.<RowType.Field>builder()
                                        .add(RowType.field("longField", BIGINT))
                                        .add(RowType.field("stringField", VARCHAR))
                                        .build()).getTypeSignature())
                        ))))
                .add(RowType.field("nestedRow", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(RowType.field("longField", BIGINT))
                        .add(RowType.field("stringField", VARCHAR))
                        .build())))
                .add(RowType.field("stringField", VARCHAR))
                .add(RowType.field("structedField",
                        decoderFactory.getTypeManager().getParameterizedType(StandardTypes.MAP,
                                ImmutableList.of(TypeSignatureParameter.typeParameter(VarcharType.VARCHAR.getTypeSignature()),
                                        TypeSignatureParameter.typeParameter(new ArrayType(BIGINT).getTypeSignature())))))
                .build());

        PulsarColumnHandle columnHandle = new PulsarColumnHandle(getPulsarConnectorId().toString(),
                "compositeRow", columnType, false, false, "compositeRow", null, null, PulsarColumnHandle.HandleKeyValueType.NONE);

        checkRowValues(getBlock(decodedRow, columnHandle), columnHandle.getType(), fieldValue);
    }

    @Test(singleThreaded = true)
    public void testCyclicDefinitionDetect() {
        AvroSchema cyclicSchema = AvroSchema.of(DecoderTestMessage.CyclicFoo.class);
        PrestoException exception = expectThrows(PrestoException.class,
                () -> {
                    decoderFactory.extractColumnMetadata(topicName, cyclicSchema.getSchemaInfo(),
                            PulsarColumnHandle.HandleKeyValueType.NONE);
                });

        assertEquals("Topic "
                + topicName.toString() + " schema may contains cyclic definitions.", exception.getMessage());

    }

}
