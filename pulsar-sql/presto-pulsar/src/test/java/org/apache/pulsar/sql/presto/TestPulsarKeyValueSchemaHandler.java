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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.prestosql.spi.connector.ColumnMetadata;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.api.raw.RawMessageImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


/**
 * Unit test for KeyValueSchemaHandler
 */
@Slf4j
public class TestPulsarKeyValueSchemaHandler {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    private Schema<KeyValue<String, Integer>> schema1 =
            Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.INLINE);

    private Schema<KeyValue<String, Foo>> schema2 =
            Schema.KeyValue(Schema.STRING, Schema.JSON(Foo.class), KeyValueEncodingType.INLINE);

    private Schema<KeyValue<Boo, Long>> schema3 =
            Schema.KeyValue(Schema.AVRO(Boo.class), Schema.INT64, KeyValueEncodingType.SEPARATED);

    private Schema<KeyValue<Boo, Foo>> schema4 =
            Schema.KeyValue(Schema.JSON(Boo.class), Schema.AVRO(Foo.class), KeyValueEncodingType.SEPARATED);

    private final static TopicName topicName = TopicName.get("persistent://public/default/kv-test");

    private final static Foo foo;

    private final static Boo boo;

    private final Integer KEY_FIELD_NAME_PREFIX_LENGTH = PulsarColumnMetadata.KEY_SCHEMA_COLUMN_PREFIX.length();

    static {
        foo = new Foo();
        foo.field1 = "field1-value";
        foo.field2 = 20;

        boo = new Boo();
        boo.field1 = "field1-value";
        boo.field2 = true;
        boo.field3 = 10.2;
    }


    @Test
    public void testSchema1() throws IOException {
        final String keyData = "test-key";
        final Integer valueData = 10;
        List<ColumnMetadata> columnMetadataList =
                PulsarMetadata.getPulsarColumns(topicName, schema1.getSchemaInfo(),
                        true,   null);
        int keyCount = 0;
        int valueCount = 0;
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;
            if (pulsarColumnMetadata.isKey()) {
                keyCount++;
            } else if (pulsarColumnMetadata.isValue()) {
                valueCount++;
            }
        }
        Assert.assertEquals(keyCount, 1);
        Assert.assertEquals(valueCount, 1);

        List<PulsarColumnHandle> columnHandleList = getColumnHandlerList(columnMetadataList);

        KeyValueSchemaHandler keyValueSchemaHandler =
                new KeyValueSchemaHandler(null, null,schema1.getSchemaInfo(), columnHandleList);

        RawMessageImpl message = mock(RawMessageImpl.class);
        Mockito.when(message.getData()).thenReturn(
                Unpooled.wrappedBuffer(schema1.encode(new KeyValue<>(keyData, valueData)))
        );

        KeyValue<ByteBuf, ByteBuf> byteBufKeyValue = getKeyValueByteBuf(message, schema1);
        Object object = keyValueSchemaHandler.deserialize(byteBufKeyValue.getKey(), byteBufKeyValue.getValue(), null);
        Assert.assertEquals(keyValueSchemaHandler.extractField(0, object), keyData);
        Assert.assertEquals(keyValueSchemaHandler.extractField(1, object), valueData);
    }

    @Test
    public void testSchema2() throws IOException {
        final String keyData = "test-key";

        List<ColumnMetadata> columnMetadataList =
                PulsarMetadata.getPulsarColumns(topicName, schema2.getSchemaInfo(),
                        true, null);
        int keyCount = 0;
        int valueCount = 0;
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;
            if (pulsarColumnMetadata.isKey()) {
                keyCount++;
            } else if (pulsarColumnMetadata.isValue()) {
                valueCount++;
            }
        }
        Assert.assertEquals(keyCount, 1);
        Assert.assertEquals(valueCount, 2);

        List<PulsarColumnHandle> columnHandleList = getColumnHandlerList(columnMetadataList);

        RawMessage message = mock(RawMessage.class);
        Mockito.when(message.getData()).thenReturn(
                Unpooled.wrappedBuffer(schema2.encode(new KeyValue<>(keyData, foo)))
        );


        KeyValueSchemaHandler keyValueSchemaHandler =
                new KeyValueSchemaHandler(null, null, schema2.getSchemaInfo(), columnHandleList);

        KeyValue<ByteBuf, ByteBuf> byteBufKeyValue = getKeyValueByteBuf(message, schema2);
        Object object = keyValueSchemaHandler.deserialize(byteBufKeyValue.getKey(), byteBufKeyValue.getValue(), null);
        Assert.assertEquals(keyValueSchemaHandler.extractField(0, object), keyData);
        Assert.assertEquals(keyValueSchemaHandler.extractField(1, object),
                foo.getValue(columnHandleList.get(1).getName()));
        Assert.assertEquals(keyValueSchemaHandler.extractField(2, object),
                foo.getValue(columnHandleList.get(2).getName()));
    }

    @Test
    public void testSchema3() throws IOException {
        final Boo boo = new Boo();
        boo.field1 = "field1-value";
        boo.field2 = true;
        boo.field3 = 10.2;
        final Long valueData = 999999L;

        List<ColumnMetadata> columnMetadataList =
                PulsarMetadata.getPulsarColumns(topicName, schema3.getSchemaInfo(),
                        true, null);
        int keyCount = 0;
        int valueCount = 0;
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;
            if (pulsarColumnMetadata.isKey()) {
                keyCount++;
            } else if (pulsarColumnMetadata.isValue()) {
                valueCount++;
            }
        }
        Assert.assertEquals(keyCount, 3);
        Assert.assertEquals(valueCount, 1);

        List<PulsarColumnHandle> columnHandleList = getColumnHandlerList(columnMetadataList);

        PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider = mock(PulsarSqlSchemaInfoProvider.class);

        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schema3.getSchemaInfo());

        AvroSchemaHandler avroSchemaHandler =
                new AvroSchemaHandler(pulsarSqlSchemaInfoProvider, kvSchemaInfo.getKey(), columnHandleList);
        PulsarPrimitiveSchemaHandler pulsarPrimitiveSchemaHandler =
                new PulsarPrimitiveSchemaHandler(kvSchemaInfo.getValue());
        KeyValueSchemaHandler keyValueSchemaHandler =
                new KeyValueSchemaHandler(avroSchemaHandler, pulsarPrimitiveSchemaHandler, columnHandleList);

        RawMessage message = mock(RawMessage.class);
        Mockito.when(message.getKeyBytes()).thenReturn(
                Optional.of(Unpooled.wrappedBuffer(
                    ((KeyValueSchema) schema3).getKeySchema().encode(boo)
                ))
        );
        Mockito.when(message.getData()).thenReturn(
                Unpooled.wrappedBuffer(schema3.encode(new KeyValue<>(boo, valueData)))
        );

        KeyValue<ByteBuf, ByteBuf> byteBufKeyValue = getKeyValueByteBuf(message, schema3);
        Integer a = 1;
        Object object = keyValueSchemaHandler.deserialize(byteBufKeyValue.getKey(), byteBufKeyValue.getValue(), null);
        Assert.assertEquals(keyValueSchemaHandler.extractField(0, object).toString(),
                boo.getValue(columnHandleList.get(0).getName().substring(KEY_FIELD_NAME_PREFIX_LENGTH)));
        Assert.assertEquals(keyValueSchemaHandler.extractField(1, object),
                boo.getValue(columnHandleList.get(1).getName().substring(KEY_FIELD_NAME_PREFIX_LENGTH)));
        Assert.assertEquals(keyValueSchemaHandler.extractField(2, object),
                boo.getValue(columnHandleList.get(2).getName().substring(KEY_FIELD_NAME_PREFIX_LENGTH)));
        Assert.assertEquals(keyValueSchemaHandler.extractField(3, object), valueData);
    }

    @Test
    public void testSchema4() throws IOException {
        List<ColumnMetadata> columnMetadataList =
                PulsarMetadata.getPulsarColumns(topicName, schema4.getSchemaInfo(),
                        true, null);
        int keyCount = 0;
        int valueCount = 0;
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;
            if (pulsarColumnMetadata.isKey()) {
                keyCount++;
            } else if (pulsarColumnMetadata.isValue()) {
                valueCount++;
            }
        }
        Assert.assertEquals(keyCount, 3);
        Assert.assertEquals(valueCount, 2);

        List<PulsarColumnHandle> columnHandleList = getColumnHandlerList(columnMetadataList);

        PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider = mock(PulsarSqlSchemaInfoProvider.class);

        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schema4.getSchemaInfo());

        AvroSchemaHandler avroSchemaHandler =
                new AvroSchemaHandler(pulsarSqlSchemaInfoProvider, kvSchemaInfo.getValue(), columnHandleList);
        JSONSchemaHandler jsonSchemaHandler = new JSONSchemaHandler(columnHandleList);
        KeyValueSchemaHandler keyValueSchemaHandler =
                new KeyValueSchemaHandler(jsonSchemaHandler, avroSchemaHandler, columnHandleList);


        RawMessage message = mock(RawMessage.class);
        Mockito.when(message.getKeyBytes()).thenReturn(
                Optional.of(Unpooled.wrappedBuffer(
                        ((KeyValueSchema) schema4).getKeySchema().encode(boo)
                ))
        );
        Mockito.when(message.getData()).thenReturn(
                Unpooled.wrappedBuffer(schema4.encode(new KeyValue<>(boo, foo)))
        );

        KeyValue<ByteBuf, ByteBuf> byteBufKeyValue = getKeyValueByteBuf(message, schema4);
        Object object = keyValueSchemaHandler.deserialize(byteBufKeyValue.getKey(), byteBufKeyValue.getValue(), null);
        Assert.assertEquals(keyValueSchemaHandler.extractField(0, object).toString(),
                boo.getValue(columnHandleList.get(0).getName().substring(KEY_FIELD_NAME_PREFIX_LENGTH)));
        Assert.assertEquals(keyValueSchemaHandler.extractField(1, object),
                boo.getValue(columnHandleList.get(1).getName().substring(KEY_FIELD_NAME_PREFIX_LENGTH)));
        Assert.assertEquals(keyValueSchemaHandler.extractField(2, object),
                boo.getValue(columnHandleList.get(2).getName().substring(KEY_FIELD_NAME_PREFIX_LENGTH)));
        Assert.assertEquals(keyValueSchemaHandler.extractField(3, object).toString(),
                foo.getValue(columnHandleList.get(3).getName()));
        Assert.assertEquals(keyValueSchemaHandler.extractField(4, object).toString(),
                foo.getValue(columnHandleList.get(4).getName()) + "");
    }

    private List<PulsarColumnHandle> getColumnHandlerList(List<ColumnMetadata> columnMetadataList) {
        List<PulsarColumnHandle> columnHandleList = new LinkedList<>();

        columnMetadataList.forEach(columnMetadata -> {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;
            PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle(
                    "connectorId",
                    pulsarColumnMetadata.getNameWithCase(),
                    pulsarColumnMetadata.getType(),
                    pulsarColumnMetadata.isHidden(),
                    pulsarColumnMetadata.isInternal(),
                    pulsarColumnMetadata.getFieldNames(),
                    pulsarColumnMetadata.getPositionIndices(),
                    pulsarColumnMetadata.getHandleKeyValueType());
            columnHandleList.add(pulsarColumnHandle);
        });

        return columnHandleList;
    }

    public KeyValue<ByteBuf, ByteBuf> getKeyValueByteBuf(RawMessage message, Schema schema) {
        KeyValueEncodingType encodingType = KeyValueSchemaInfo.decodeKeyValueEncodingType(schema.getSchemaInfo());
        ByteBuf keyByteBuf = null;
        if (Objects.equals(KeyValueEncodingType.SEPARATED, encodingType)) {
            if (message.getKeyBytes().isPresent()) {
                keyByteBuf = message.getKeyBytes().get();
            } else {
                keyByteBuf = null;
            }
        } else {
            keyByteBuf = null;
        }
        return new KeyValue<>(keyByteBuf, Unpooled.wrappedBuffer(message.getData()));
    }

    @Data
    static class Foo {
        private String field1;
        private Integer field2;

        public Object getValue(String fieldName) {
            switch (fieldName) {
                case "field1":
                    return field1;
                case "field2":
                    return field2 == null ? null : new Long(field2);
                default:
                    return null;
            }
        }
    }

    @Data
    static class Boo {
        private String field1;
        private Boolean field2;
        private Double field3;

        public Object getValue(String fieldName) {
            switch (fieldName) {
                case "field1":
                    return field1;
                case "field2":
                    return field2;
                case "field3":
                    return field3  == null ? null : field3.doubleValue();
                default:
                    return null;
            }
        }

    }

}
