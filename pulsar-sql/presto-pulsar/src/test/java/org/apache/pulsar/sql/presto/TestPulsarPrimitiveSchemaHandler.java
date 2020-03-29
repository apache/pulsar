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
package org.apache.pulsar.sql.presto;

import com.facebook.presto.spi.ColumnMetadata;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class TestPulsarPrimitiveSchemaHandler {

    private static final TopicName stringTopicName = TopicName.get("persistent", "tenant-1", "ns-1", "topic-1");
    @Test
    public void testPulsarPrimitiveSchemaHandler() {
        PulsarPrimitiveSchemaHandler pulsarPrimitiveSchemaHandler;
        RawMessage rawMessage = mock(RawMessage.class);
        SchemaInfo schemaInfoInt8 = SchemaInfo.builder().type(SchemaType.INT8).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoInt8);
        byte int8Value = 1;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(ByteSchema.of().encode(int8Value)));
        Assert.assertEquals(int8Value, (byte)pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoInt16 = SchemaInfo.builder().type(SchemaType.INT16).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoInt16);
        short int16Value = 2;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(ShortSchema.of().encode(int16Value)));
        Assert.assertEquals(int16Value, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoInt32 = SchemaInfo.builder().type(SchemaType.INT32).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoInt32);
        int int32Value = 2;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(IntSchema.of().encode(int32Value)));
        Assert.assertEquals(int32Value, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoInt64 = SchemaInfo.builder().type(SchemaType.INT64).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoInt64);
        long int64Value = 2;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(LongSchema.of().encode(int64Value)));
        Assert.assertEquals(int64Value, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoString = SchemaInfo.builder().type(SchemaType.STRING).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoString);
        String stringValue = "test";
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(StringSchema.utf8().encode(stringValue)));
        Assert.assertEquals(stringValue, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoFloat = SchemaInfo.builder().type(SchemaType.FLOAT).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoFloat);
        float floatValue = 0.2f;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(FloatSchema.of().encode(floatValue)));
        Assert.assertEquals(floatValue, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoDouble = SchemaInfo.builder().type(SchemaType.DOUBLE).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoDouble);
        double doubleValue = 0.22d;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(DoubleSchema.of().encode(doubleValue)));
        Assert.assertEquals(doubleValue, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoBoolean = SchemaInfo.builder().type(SchemaType.BOOLEAN).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoBoolean);
        boolean booleanValue = true;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(BooleanSchema.of().encode(booleanValue)));
        Assert.assertEquals(booleanValue, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoBytes = SchemaInfo.builder().type(SchemaType.BYTES).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoBytes);
        byte[] bytesValue = new byte[1];
        bytesValue[0] = 1;
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(BytesSchema.of().encode(bytesValue)));
        Assert.assertEquals(bytesValue, pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData()));

        SchemaInfo schemaInfoDate = SchemaInfo.builder().type(SchemaType.DATE).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoDate);
        Date dateValue = new Date(System.currentTimeMillis());
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(DateSchema.of().encode(dateValue)));
        Object dateDeserializeValue = pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData());
        Assert.assertEquals(dateValue.getTime(), dateDeserializeValue);

        SchemaInfo schemaInfoTime = SchemaInfo.builder().type(SchemaType.TIME).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoTime);
        Time timeValue = new Time(System.currentTimeMillis());
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(TimeSchema.of().encode(timeValue)));
        Object timeDeserializeValue = pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData());
        Assert.assertEquals(timeValue.getTime(), timeDeserializeValue);

        SchemaInfo schemaInfoTimestamp = SchemaInfo.builder().type(SchemaType.TIMESTAMP).build();
        pulsarPrimitiveSchemaHandler = new PulsarPrimitiveSchemaHandler(schemaInfoTimestamp);
        Timestamp timestampValue = new Timestamp(System.currentTimeMillis());
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(TimestampSchema.of().encode(timestampValue)));
        Object timestampDeserializeValue = pulsarPrimitiveSchemaHandler.deserialize(rawMessage.getData());
        Assert.assertEquals(timestampValue.getTime(), timestampDeserializeValue);
    }

    @Test
    public void testNewPulsarPrimitiveSchemaHandler() {
        RawMessage rawMessage = mock(RawMessage.class);
        SchemaHandler schemaHandler = PulsarSchemaHandlers.newPulsarSchemaHandler(
                StringSchema.utf8().getSchemaInfo(),
                null);

        String stringValue = "test";
        when(rawMessage.getData()).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(StringSchema.utf8().encode(stringValue)));

        Object deserializeValue = schemaHandler.deserialize(rawMessage.getData());
        Assert.assertEquals(stringValue, (String)deserializeValue);
        Assert.assertEquals(stringValue, (String)deserializeValue);

    }

    @Test
    public void testNewColumnMetadata() {
        List<ColumnMetadata> columnMetadataList = PulsarMetadata.getPulsarColumns(stringTopicName,
                StringSchema.utf8().getSchemaInfo(), false, null);
        Assert.assertEquals(columnMetadataList.size(), 1);
        ColumnMetadata columnMetadata = columnMetadataList.get(0);
        Assert.assertEquals("__value__", columnMetadata.getName());
        Assert.assertEquals("varchar", columnMetadata.getType().toString());
    }
}