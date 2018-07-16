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
package org.apache.pulsar.client.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ProtobufSchemaTest {

    private static final String NAME = "foo";

    private static final String EXPECTED_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"TestMessage\"," +
            "\"namespace\":\"org.apache.pulsar.client.schema.proto.Test$\",\"fields\":[{\"name\":\"stringField\"," +
            "\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"}," +
            "{\"name\":\"doubleField\",\"type\":\"double\",\"default\":0},{\"name\":\"intField\",\"type\":\"int\"," +
            "\"default\":0},{\"name\":\"testEnum\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\"," +
            "\"symbols\":[\"SHARED\",\"FAILOVER\"]},\"default\":\"SHARED\"},{\"name\":\"nestedField\"," +
            "\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SubMessage\",\"fields\":[{\"name\":\"foo\"," +
            "\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"bar\"," +
            "\"type\":\"double\",\"default\":0}]}],\"default\":null}]}";

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
}
