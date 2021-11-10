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
package org.apache.pulsar.sql.presto.decoder.primitive;

import io.airlift.slice.Slices;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;
import org.apache.pulsar.sql.presto.decoder.AbstractDecoderTester;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static org.apache.pulsar.sql.presto.TestPulsarConnector.getPulsarConnectorId;

public class TestPrimitiveDecoder extends AbstractDecoderTester {

    public static final String PRIMITIVE_COLUMN_NAME = "__value__";

    @BeforeMethod
    public void init() {
        decoderTestUtil = new PrimitiveDecoderTestUtil();
        super.init();
    }

    @Test(singleThreaded = true)
    public void testPrimitiveType() {

        byte int8Value = 1;
        SchemaInfo schemaInfoInt8 = SchemaInfo.builder().type(SchemaType.INT8).build();
        Schema schemaInt8 = Schema.getSchema(schemaInfoInt8);
        List<PulsarColumnHandle> pulsarColumnHandleInt8 = getColumnColumnHandles(topicName, schemaInfoInt8, PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderInt8 = decoderFactory.createRowDecoder(topicName, schemaInfoInt8,
                new HashSet<>(pulsarColumnHandleInt8));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowInt8 =
                pulsarRowDecoderInt8.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaInt8.encode(int8Value))).get();
        checkValue(decodedRowInt8, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, TINYINT, false, false, PRIMITIVE_COLUMN_NAME, null, null, PulsarColumnHandle.HandleKeyValueType.NONE), int8Value);

        short int16Value = 2;
        SchemaInfo schemaInfoInt16 = SchemaInfo.builder().type(SchemaType.INT16).build();
        Schema schemaInt16 = Schema.getSchema(schemaInfoInt16);
        List<PulsarColumnHandle> pulsarColumnHandleInt16 = getColumnColumnHandles(topicName, schemaInfoInt16, PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderInt16 = decoderFactory.createRowDecoder(topicName, schemaInfoInt16,
                new HashSet<>(pulsarColumnHandleInt16));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowInt16 =
                pulsarRowDecoderInt16.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaInt16.encode(int16Value))).get();
        checkValue(decodedRowInt16, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, SMALLINT, false, false, PRIMITIVE_COLUMN_NAME, null, null, PulsarColumnHandle.HandleKeyValueType.NONE), int16Value);

        int int32Value = 2;
        SchemaInfo schemaInfoInt32 = SchemaInfo.builder().type(SchemaType.INT32).build();
        Schema schemaInt32 = Schema.getSchema(schemaInfoInt32);
        List<PulsarColumnHandle> pulsarColumnHandleInt32 = getColumnColumnHandles(topicName, schemaInfoInt32,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderInt32 = decoderFactory.createRowDecoder(topicName, schemaInfoInt32,
                new HashSet<>(pulsarColumnHandleInt32));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowInt32 =
                pulsarRowDecoderInt32.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaInt32.encode(int32Value))).get();
        checkValue(decodedRowInt32, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, INTEGER, false, false, PRIMITIVE_COLUMN_NAME, null, null, PulsarColumnHandle.HandleKeyValueType.NONE), int32Value);

        long int64Value = 2;
        SchemaInfo schemaInfoInt64 = SchemaInfo.builder().type(SchemaType.INT64).build();
        Schema schemaInt64 = Schema.getSchema(schemaInfoInt64);
        List<PulsarColumnHandle> pulsarColumnHandleInt64 = getColumnColumnHandles(topicName, schemaInfoInt64,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderInt64 = decoderFactory.createRowDecoder(topicName, schemaInfoInt64,
                new HashSet<>(pulsarColumnHandleInt64));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowInt64 =
                pulsarRowDecoderInt64.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaInt64.encode(int64Value))).get();
        checkValue(decodedRowInt64, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, BIGINT, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), int64Value);

        String stringValue = "test";
        SchemaInfo schemaInfoString = SchemaInfo.builder().type(SchemaType.STRING).build();
        Schema schemaString = Schema.getSchema(schemaInfoString);
        List<PulsarColumnHandle> pulsarColumnHandleString = getColumnColumnHandles(topicName, schemaInfoString,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderString = decoderFactory.createRowDecoder(topicName, schemaInfoString,
                new HashSet<>(pulsarColumnHandleString));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowString =
                pulsarRowDecoderString.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaString.encode(stringValue))).get();
        checkValue(decodedRowString, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, VARCHAR, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), stringValue);

        float floatValue = 0.2f;
        SchemaInfo schemaInfoFloat = SchemaInfo.builder().type(SchemaType.FLOAT).build();
        Schema schemaFloat = Schema.getSchema(schemaInfoFloat);
        List<PulsarColumnHandle> pulsarColumnHandleFloat = getColumnColumnHandles(topicName, schemaInfoFloat,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderFloat = decoderFactory.createRowDecoder(topicName, schemaInfoFloat,
                new HashSet<>(pulsarColumnHandleFloat));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowFloat =
                pulsarRowDecoderFloat.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaFloat.encode(floatValue))).get();
        checkValue(decodedRowFloat, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, REAL, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), Long.valueOf(Float.floatToIntBits(floatValue)));

        double doubleValue = 0.22d;
        SchemaInfo schemaInfoDouble = SchemaInfo.builder().type(SchemaType.DOUBLE).build();
        Schema schemaDouble = Schema.getSchema(schemaInfoDouble);
        List<PulsarColumnHandle> pulsarColumnHandleDouble = getColumnColumnHandles(topicName, schemaInfoDouble,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderDouble = decoderFactory.createRowDecoder(topicName, schemaInfoDouble,
                new HashSet<>(pulsarColumnHandleDouble));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowDouble =
                pulsarRowDecoderDouble.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaDouble.encode(doubleValue))).get();
        checkValue(decodedRowDouble, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, DOUBLE, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), doubleValue);

        boolean booleanValue = true;
        SchemaInfo schemaInfoBoolean = SchemaInfo.builder().type(SchemaType.BOOLEAN).build();
        Schema schemaBoolean = Schema.getSchema(schemaInfoBoolean);
        List<PulsarColumnHandle> pulsarColumnHandleBoolean = getColumnColumnHandles(topicName, schemaInfoBoolean,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderBoolean = decoderFactory.createRowDecoder(topicName, schemaInfoBoolean,
                new HashSet<>(pulsarColumnHandleBoolean));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowBoolean =
                pulsarRowDecoderBoolean.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaBoolean.encode(booleanValue))).get();
        checkValue(decodedRowBoolean, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, BOOLEAN, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), booleanValue);

        byte[] bytesValue = new byte[1];
        bytesValue[0] = 1;
        SchemaInfo schemaInfoBytes = SchemaInfo.builder().type(SchemaType.BYTES).build();
        Schema schemaBytes = Schema.getSchema(schemaInfoBytes);
        List<PulsarColumnHandle> pulsarColumnHandleBytes = getColumnColumnHandles(topicName, schemaInfoBytes,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderBytes = decoderFactory.createRowDecoder(topicName, schemaInfoBytes,
                new HashSet<>(pulsarColumnHandleBytes));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowBytes =
                pulsarRowDecoderBytes.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaBytes.encode(bytesValue))).get();
        checkValue(decodedRowBytes, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, VARBINARY, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), Slices.wrappedBuffer(bytesValue));

        Date dateValue = new Date(System.currentTimeMillis());
        SchemaInfo schemaInfoDate = SchemaInfo.builder().type(SchemaType.DATE).build();
        Schema schemaDate = Schema.getSchema(schemaInfoDate);
        List<PulsarColumnHandle> pulsarColumnHandleDate = getColumnColumnHandles(topicName, schemaInfoDate,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderDate = decoderFactory.createRowDecoder(topicName, schemaInfoDate,
                new HashSet<>(pulsarColumnHandleDate));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowDate =
                pulsarRowDecoderDate.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaDate.encode(dateValue))).get();
        checkValue(decodedRowDate, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, DATE, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), dateValue.getTime());

        Time timeValue = new Time(System.currentTimeMillis());
        SchemaInfo schemaInfoTime = SchemaInfo.builder().type(SchemaType.TIME).build();
        Schema schemaTime = Schema.getSchema(schemaInfoTime);
        List<PulsarColumnHandle> pulsarColumnHandleTime = getColumnColumnHandles(topicName, schemaInfoTime,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderTime = decoderFactory.createRowDecoder(topicName, schemaInfoTime,
                new HashSet<>(pulsarColumnHandleTime));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowTime =
                pulsarRowDecoderTime.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaTime.encode(timeValue))).get();
        checkValue(decodedRowTime, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, TIME, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), timeValue.getTime());

        Timestamp timestampValue = new Timestamp(System.currentTimeMillis());
        SchemaInfo schemaInfoTimestamp = SchemaInfo.builder().type(SchemaType.TIMESTAMP).build();
        Schema schemaTimestamp = Schema.getSchema(schemaInfoTimestamp);
        List<PulsarColumnHandle> pulsarColumnHandleTimestamp = getColumnColumnHandles(topicName, schemaInfoTimestamp,
                PulsarColumnHandle.HandleKeyValueType.NONE, false, decoderFactory);
        PulsarRowDecoder pulsarRowDecoderTimestamp = decoderFactory.createRowDecoder(topicName, schemaInfoTimestamp,
                new HashSet<>(pulsarColumnHandleTimestamp));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRowTimestamp =
                pulsarRowDecoderTimestamp.decodeRow(io.netty.buffer.Unpooled
                        .copiedBuffer(schemaTimestamp.encode(timestampValue))).get();
        checkValue(decodedRowTimestamp, new PulsarColumnHandle(getPulsarConnectorId().toString(),
                PRIMITIVE_COLUMN_NAME, TIMESTAMP, false, false, PRIMITIVE_COLUMN_NAME, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE), timestampValue.getTime());

    }

}
