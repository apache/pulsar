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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.schema.SchemaType;
import org.json.JSONException;
import org.json.JSONObject;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Decode a base64-encoded Protobuf FileDescriptorSet into a canonical JSON tree.
     * @param b64 a base64 string
     * @return a normalized JSON tree that can be compared with JSONAssert, ensuring deterministic
     * equality checks regardless of field ordering.
     * @throws IllegalArgumentException if b64 isn't valid Base64
     * @throws InvalidProtocolBufferException if the decoded bytes are not a valid FileDescriptorSet
     * @throws JSONException if the JSON string is invalid
     */
    private static JSONObject fdsToJson(String b64)
            throws IllegalArgumentException, InvalidProtocolBufferException, JSONException {
        DescriptorProtos.FileDescriptorSet fds =
            DescriptorProtos.FileDescriptorSet.parseFrom(Base64.getDecoder().decode(b64));
        String json = JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .print(fds);
        return new JSONObject(json);
    }

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
    public void testSchema() throws Exception {
        ProtobufNativeSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema =
                ProtobufNativeSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);

        assertEquals(protobufSchema.getSchemaInfo().getType(), SchemaType.PROTOBUF_NATIVE);

        assertNotNull(ProtobufNativeSchemaUtils.deserialize(protobufSchema.getSchemaInfo().getSchema()));

        // Parse the actual/expected JSON into trees
        String actualJson   = new String(protobufSchema.getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
        JsonNode actualRoot   = MAPPER.readTree(actualJson);
        JsonNode expectedRoot = MAPPER.readTree(EXPECTED_SCHEMA_JSON);

        // Extract and validate the FileDescriptorSet field for semantic comparison
        // (When decoded, Protobuf descriptors can serialize fields in varying orders
        // causing hard coded string comparisons to fail)
        String fdSetField = "fileDescriptorSet";
        JsonNode actualB64Node = actualRoot.path(fdSetField);
        JsonNode expectedB64Node = expectedRoot.path(fdSetField);
        Assert.assertFalse(actualB64Node.isMissingNode());
        Assert.assertFalse(expectedB64Node.isMissingNode());
        Assert.assertTrue(actualB64Node.isValueNode());
        Assert.assertTrue(expectedB64Node.isValueNode());

        // Decode FileDescriptorSets to JSON and compare semantically (order-insensitive)
        JSONObject actualFdsObj   = fdsToJson(actualB64Node.asText());
        JSONObject expectedFdsObj = fdsToJson(expectedB64Node.asText());
        JSONAssert.assertEquals(
                "FileDescriptorSet mismatch: decoded Protobuf descriptors have mismatched schema contents",
                expectedFdsObj,
                actualFdsObj,
                JSONCompareMode.NON_EXTENSIBLE
        );

        // Remove the already verified field and compare remaining schema attributes, order does not matter
        ((ObjectNode) actualRoot).remove(fdSetField);
        ((ObjectNode) expectedRoot).remove(fdSetField);
        JSONAssert.assertEquals(
                "Schema metadata mismatch: remaining JSON fields differ after verifying FileDescriptorSet",
                MAPPER.writeValueAsString(expectedRoot),
                MAPPER.writeValueAsString(actualRoot),
                JSONCompareMode.NON_EXTENSIBLE
        );
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
