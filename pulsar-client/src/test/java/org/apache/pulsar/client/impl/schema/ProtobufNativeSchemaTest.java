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
package org.apache.pulsar.client.impl.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.google.protobuf.Descriptors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ProtobufNativeSchemaTest {


    private static final String EXPECTED_SCHEMA_JSON = "{\"fileDescriptorSet\":\"CtMDCgpUZXN0LnByb3RvEgVwcm90bxoSRXh0Z"
            + "XJuYWxUZXN0LnByb3RvImUKClN1Yk1lc3NhZ2USCwoDZm9vGAEgASgJEgsKA2JhchgCIAEoARo9Cg1OZXN0ZWRNZXNzYWdlEgsKA3Vy"
            + "bBgBIAEoCRINCgV0aXRsZRgCIAEoCRIQCghzbmlwcGV0cxgDIAMoCSLlAQoLVGVzdE1lc3NhZ2USEwoLc3RyaW5nRmllbGQYASABKAk"
            + "SEwoLZG91YmxlRmllbGQYAiABKAESEAoIaW50RmllbGQYBiABKAUSIQoIdGVzdEVudW0YBCABKA4yDy5wcm90by5UZXN0RW51bRImCg"
            + "tuZXN0ZWRGaWVsZBgFIAEoCzIRLnByb3RvLlN1Yk1lc3NhZ2USFQoNcmVwZWF0ZWRGaWVsZBgKIAMoCRI4Cg9leHRlcm5hbE1lc3NhZ"
            + "2UYCyABKAsyHy5wcm90by5leHRlcm5hbC5FeHRlcm5hbE1lc3NhZ2UqJAoIVGVzdEVudW0SCgoGU0hBUkVEEAASDAoIRkFJTE9WRVIQ"
            + "AUItCiVvcmcuYXBhY2hlLnB1bHNhci5jbGllbnQuc2NoZW1hLnByb3RvQgRUZXN0YgZwcm90bzMKoAEKEkV4dGVybmFsVGVzdC5wcm9"
            + "0bxIOcHJvdG8uZXh0ZXJuYWwiOwoPRXh0ZXJuYWxNZXNzYWdlEhMKC3N0cmluZ0ZpZWxkGAEgASgJEhMKC2RvdWJsZUZpZWxkGAIgAS"
            + "gBQjUKJW9yZy5hcGFjaGUucHVsc2FyLmNsaWVudC5zY2hlbWEucHJvdG9CDEV4dGVybmFsVGVzdGIGcHJvdG8z\",\"rootMessageT"
            + "ypeName\":\"proto.TestMessage\",\"rootFileDescriptorName\":\"Test.proto\"}";

    @Test
    public void testEncodeAndDecode() {
        final String stringFieldValue = "StringFieldValue";
        org.apache.pulsar.client.schema.proto.Test.TestMessage testMessage = org.apache.pulsar.client.schema.proto
                .Test.TestMessage.newBuilder().setStringField(stringFieldValue).build();
        ProtobufNativeSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema =
                ProtobufNativeSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);

        byte[] bytes = protobufSchema.encode(testMessage);
        org.apache.pulsar.client.schema.proto.Test.TestMessage message = protobufSchema.decode(bytes);

        assertEquals(message.getStringField(), stringFieldValue);
    }

    @Test
    public void testSchema() {
        ProtobufNativeSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema =
                ProtobufNativeSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);

        assertEquals(protobufSchema.getSchemaInfo().getType(), SchemaType.PROTOBUF_NATIVE);

        assertNotNull(ProtobufNativeSchemaUtils.deserialize(protobufSchema.getSchemaInfo().getSchema()));
        assertEquals(new String(protobufSchema.getSchemaInfo().getSchema(),
                StandardCharsets.UTF_8), EXPECTED_SCHEMA_JSON);
    }

    @Test
    public void testGenericOf() {
        try {
            ProtobufNativeSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufNativeSchema =
                    ProtobufNativeSchema.ofGenericClass(org.apache.pulsar.client.schema.proto.Test.TestMessage.class,
                    new HashMap<>());
        } catch (Exception e) {
            Assert.fail("Should not construct a ProtobufShema over a non-protobuf-generated class");
        }

        try {
            ProtobufSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema =
                    ProtobufSchema.ofGenericClass(String.class,
                    Collections.emptyMap());
            Assert.fail("Should not construct a ProtobufNativeShema over a non-protobuf-generated class");
        } catch (Exception e) {

        }
    }


    @Test
    public void testDecodeByteBuf() {
        ProtobufNativeSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema =
                ProtobufNativeSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);
        org.apache.pulsar.client.schema.proto.Test.TestMessage testMessage =
                org.apache.pulsar.client.schema.proto.Test.TestMessage.newBuilder().build();
        byte[] bytes =
                protobufSchema.encode(org.apache.pulsar.client.schema.proto.Test.TestMessage.newBuilder().build());
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(bytes.length);
        byteBuf.writeBytes(bytes);

        assertEquals(testMessage, protobufSchema.decode(byteBuf));

    }

    @Test
    public void testGetNativeSchema()  {
        ProtobufNativeSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema =
                ProtobufNativeSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);
        Descriptors.Descriptor nativeSchema = (Descriptors.Descriptor) protobufSchema.getNativeSchema().get();
        assertNotNull(nativeSchema);
    }

}
