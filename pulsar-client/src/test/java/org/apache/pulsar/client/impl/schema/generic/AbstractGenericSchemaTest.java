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
package org.apache.pulsar.client.impl.schema.generic;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Unit testing AbstractGenericSchema for non-avroBasedGenericSchema.
 */
@Slf4j
public class AbstractGenericSchemaTest {

    @Test
    public void testGenericProtobufNativeSchema() {
        Schema<org.apache.pulsar.client.schema.proto.Test.TestMessage> encodeSchema = Schema.PROTOBUF_NATIVE(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);
        GenericSchema decodeSchema = GenericProtobufNativeSchema.of(encodeSchema.getSchemaInfo());

        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testAutoProtobufNativeSchema() {
        // configure the schema info provider
        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        GenericSchema genericProtobufNativeSchema = GenericProtobufNativeSchema.of(Schema.PROTOBUF_NATIVE(org.apache.pulsar.client.schema.proto.Test.TestMessage.class).getSchemaInfo());
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(genericProtobufNativeSchema.getSchemaInfo()));

        // configure encode schema
        Schema<org.apache.pulsar.client.schema.proto.Test.TestMessage> encodeSchema = Schema.PROTOBUF_NATIVE(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);
        // configure decode schema
        AutoConsumeSchema decodeSchema = new AutoConsumeSchema();
        decodeSchema.configureSchemaInfo("test-topic", "topic", encodeSchema.getSchemaInfo());
        decodeSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);

        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    private void testEncodeAndDecodeGenericRecord(Schema<org.apache.pulsar.client.schema.proto.Test.TestMessage> encodeSchema,
                                                  Schema<GenericRecord> decodeSchema) {
        int numRecords = 10;
        for (int i = 0; i < numRecords; i++) {
            org.apache.pulsar.client.schema.proto.Test.TestMessage testMessage = newTestMessage(i);
            byte[] data = encodeSchema.encode(testMessage);

            log.info("Decoding : {}", new String(data, UTF_8));

            GenericRecord record;
            if (decodeSchema instanceof AutoConsumeSchema) {
                record = decodeSchema.decode(data, new LongSchemaVersion(0L).bytes());
            } else {
                record = decodeSchema.decode(data);
            }
            verifyTestMessageRecord(record, i);
        }
    }


    private static org.apache.pulsar.client.schema.proto.Test.TestMessage newTestMessage(int i) {
        return org.apache.pulsar.client.schema.proto.Test.TestMessage.newBuilder().setStringField("field-value-" + i)
                .setIntField(i).build();
    }

    private static void verifyTestMessageRecord(GenericRecord record, int i) {
        Object stringField = record.getField("stringField");
        assertEquals("field-value-" + i, stringField);
        Object intField = record.getField("intField");
        assertEquals(+i, intField);
    }

}
