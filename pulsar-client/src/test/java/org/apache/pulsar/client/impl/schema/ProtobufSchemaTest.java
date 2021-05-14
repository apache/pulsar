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
package org.apache.pulsar.client.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;

@Slf4j
public class ProtobufSchemaTest {

    private static final String NAME = "foo";

    private static final String EXPECTED_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"TestMessage\"," +
            "\"namespace\":\"org.apache.pulsar.client.schema.proto.Test\"," +
            "\"fields\":[{\"name\":\"stringField\",\"type\":{\"type\":\"string\"," +
            "\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"doubleField\"," +
            "\"type\":\"double\",\"default\":0},{\"name\":\"intField\",\"type\":\"int\"," +
            "\"default\":0},{\"name\":\"testEnum\",\"type\":{\"type\":\"enum\"," +
            "\"name\":\"TestEnum\",\"symbols\":[\"SHARED\",\"FAILOVER\"]}," +
            "\"default\":\"SHARED\"},{\"name\":\"nestedField\"," +
            "\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SubMessage\"," +
            "\"fields\":[{\"name\":\"foo\",\"type\":{\"type\":\"string\"," +
            "\"avro.java.string\":\"String\"},\"default\":\"\"}" +
            ",{\"name\":\"bar\",\"type\":\"double\",\"default\":0}]}]" +
            ",\"default\":null},{\"name\":\"repeatedField\",\"type\":{\"type\":\"array\"" +
            ",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},\"default\":[]}" +
            ",{\"name\":\"externalMessage\",\"type\":[\"null\",{\"type\":\"record\"" +
            ",\"name\":\"ExternalMessage\",\"namespace\":\"org.apache.pulsar.client.schema.proto.ExternalTest\"" +
            ",\"fields\":[{\"name\":\"stringField\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}," +
            "\"default\":\"\"},{\"name\":\"doubleField\",\"type\":\"double\",\"default\":0}]}],\"default\":null}]}";

    private static final String EXPECTED_PARSING_INFO = "{\"__alwaysAllowNull\":\"true\",\"__jsr310ConversionEnabled\":\"false\"," +
            "\"__PARSING_INFO__\":\"[{\\\"number\\\":1,\\\"name\\\":\\\"stringField\\\",\\\"type\\\":\\\"STRING\\\"," +
            "\\\"label\\\":\\\"LABEL_OPTIONAL\\\",\\\"definition\\\":null},{\\\"number\\\":2,\\\"name\\\":\\\"doubleField\\\"," +
            "\\\"type\\\":\\\"DOUBLE\\\",\\\"label\\\":\\\"LABEL_OPTIONAL\\\",\\\"definition\\\":null},{\\\"number\\\":6," +
            "\\\"name\\\":\\\"intField\\\",\\\"type\\\":\\\"INT32\\\",\\\"label\\\":\\\"LABEL_OPTIONAL\\\",\\\"definition\\\":null}," +
            "{\\\"number\\\":4,\\\"name\\\":\\\"testEnum\\\",\\\"type\\\":\\\"ENUM\\\",\\\"label\\\":\\\"LABEL_OPTIONAL\\\"," +
            "\\\"definition\\\":null},{\\\"number\\\":5,\\\"name\\\":\\\"nestedField\\\",\\\"type\\\":\\\"MESSAGE\\\"," +
            "\\\"label\\\":\\\"LABEL_OPTIONAL\\\",\\\"definition\\\":null},{\\\"number\\\":10,\\\"name\\\":\\\"repeatedField\\\"," +
            "\\\"type\\\":\\\"STRING\\\",\\\"label\\\":\\\"LABEL_REPEATED\\\",\\\"definition\\\":null},{\\\"number\\\":11," +
            "\\\"name\\\":\\\"externalMessage\\\",\\\"type\\\":\\\"MESSAGE\\\",\\\"label\\\":\\\"LABEL_OPTIONAL\\\",\\\"definition\\\":null}]\"}";

    @Test
    public void testEncodeAndDecode() {
        Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder().setName(NAME).build();

        ProtobufSchema<Function.FunctionDetails> protobufSchema = ProtobufSchema.of(Function.FunctionDetails.class);

        byte[] bytes = protobufSchema.encode(functionDetails);

        Function.FunctionDetails message = protobufSchema.decode(bytes);

        Assert.assertEquals(message.getName(), NAME);
    }

    @Test
    public void testSchema() {
        ProtobufSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema
                = ProtobufSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);

        Assert.assertEquals(protobufSchema.getSchemaInfo().getType(), SchemaType.PROTOBUF);

        String schemaJson = new String(protobufSchema.getSchemaInfo().getSchema());
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaJson);

        Assert.assertEquals(schema.toString(), EXPECTED_SCHEMA_JSON);
    }

    @Test
    public void testGenericOf() {
        try {
            ProtobufSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema
                    = ProtobufSchema.ofGenericClass(org.apache.pulsar.client.schema.proto.Test.TestMessage.class,
                    new HashMap<>());
        } catch (Exception e) {
            Assert.fail("Should not construct a ProtobufShema over a non-protobuf-generated class");
        }

        try {
            ProtobufSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema
                    = ProtobufSchema.ofGenericClass(String.class,
                    Collections.emptyMap());
            Assert.fail("Should not construct a ProtobufShema over a non-protobuf-generated class");
        } catch (Exception e) {

        }
    }

    @Test
    public void testParsingInfoProperty() throws JsonProcessingException {
        ProtobufSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema
                = ProtobufSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);

        Assert.assertEquals(new ObjectMapper().writeValueAsString(protobufSchema.getSchemaInfo().getProperties()), EXPECTED_PARSING_INFO);

    }

    @Test
    public void testDecodeByteBuf() throws JsonProcessingException {
        ProtobufSchema<org.apache.pulsar.client.schema.proto.Test.TestMessage> protobufSchema
                = ProtobufSchema.of(org.apache.pulsar.client.schema.proto.Test.TestMessage.class);
        org.apache.pulsar.client.schema.proto.Test.TestMessage testMessage =
                org.apache.pulsar.client.schema.proto.Test.TestMessage.newBuilder().build();
        byte[] bytes = protobufSchema.encode(org.apache.pulsar.client.schema.proto.Test.TestMessage.newBuilder().build());
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(bytes.length);
        byteBuf.writeBytes(bytes);

        Assert.assertEquals(testMessage, protobufSchema.decode(byteBuf));

    }
}
