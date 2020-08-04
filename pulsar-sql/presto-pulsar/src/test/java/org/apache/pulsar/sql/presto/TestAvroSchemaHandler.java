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

import io.netty.buffer.ByteBufAllocator;
import io.prestosql.spi.type.BigintType;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.StructSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import static org.mockito.Mockito.mock;

@Slf4j
public class TestAvroSchemaHandler {

    @Data
    public static class Foo1 {
        String field1;

        Bar bar;
    }
    @Data
    public static class Foo2 {
        String field1;

        String field2;

        Bar bar;
    }

    @Data static class Bar {
        String field1;

        String field2;
    }

    @Test
    public void testAvroSchemaHandler() throws IOException {
        List<PulsarColumnHandle> columnHandles = new ArrayList();
        RawMessage message = mock(RawMessage.class);
        Schema schema1 = ReflectData.AllowNull.get().getSchema(Foo1.class);
        PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider = mock(PulsarSqlSchemaInfoProvider.class);
        AvroSchemaHandler avroSchemaHandler = new AvroSchemaHandler(pulsarSqlSchemaInfoProvider,
                StructSchema.parseSchemaInfo(SchemaDefinition.builder().withPojo(Foo2.class).build(), SchemaType.AVRO), columnHandles);
        byte[] schemaVersion = new byte[8];
        for (int i = 0 ; i<8; i++) {
            schemaVersion[i] = 0;
        }
        ReflectDatumWriter<Foo1> writer;
        BinaryEncoder encoder = null;
        ByteArrayOutputStream byteArrayOutputStream;
        byteArrayOutputStream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
        writer = new ReflectDatumWriter<>(schema1);
        Foo1 foo1 = new Foo1();
        foo1.setField1("value1");
        foo1.setBar(new Bar());
        foo1.getBar().setField1("value1");
        writer.write(foo1, encoder);
        encoder.flush();
        when(message.getSchemaVersion()).thenReturn(schemaVersion);
        byte[] bytes =byteArrayOutputStream.toByteArray();

        when(message.getData()).thenReturn(ByteBufAllocator.DEFAULT
                .buffer(bytes.length, bytes.length).writeBytes(byteArrayOutputStream.toByteArray()));
        when(pulsarSqlSchemaInfoProvider.getSchemaByVersion(any()))
                .thenReturn(completedFuture(StructSchema.parseSchemaInfo(SchemaDefinition.builder()
                        .withPojo(Foo1.class).build(), SchemaType.AVRO)));

        Object object  = ((GenericAvroRecord)avroSchemaHandler.deserialize(message.getData(),
                message.getSchemaVersion())).getField("field1");
        Assert.assertEquals(foo1.field1, (String)object);
        String[] fields = new String[2];
        fields[0] = "bar";
        fields[1] = "field1";
        PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle("test",
                "bar.field1",
                BigintType.BIGINT,
                true,
                true,
                fields,
                new Integer[5],
                null);
        columnHandles.add(pulsarColumnHandle);
        when(message.getData()).thenReturn(ByteBufAllocator.DEFAULT
                .buffer(bytes.length, bytes.length).writeBytes(byteArrayOutputStream.toByteArray()));
        object = avroSchemaHandler.extractField(0, avroSchemaHandler.deserialize(message.getData(),
                message.getSchemaVersion()));
        Assert.assertEquals(foo1.bar.field1, (String)object);
    }
} 
