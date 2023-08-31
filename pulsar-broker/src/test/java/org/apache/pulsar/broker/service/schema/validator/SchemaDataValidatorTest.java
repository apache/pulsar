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
package org.apache.pulsar.broker.service.schema.validator;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SchemaDataValidatorTest {

    private static class Foo {
        int field;
    }

    @DataProvider(name = "primitiveSchemas")
    public static Object[][] primitiveSchemas() {
        return new Object[][] {
            { SchemaType.STRING },
            { SchemaType.BOOLEAN },
            { SchemaType.INT8 },
            { SchemaType.INT16 },
            { SchemaType.INT32 },
            { SchemaType.INT64 },
            { SchemaType.FLOAT },
            { SchemaType.DOUBLE },
            { SchemaType.DATE },
            { SchemaType.TIME },
            { SchemaType.TIMESTAMP },
            { SchemaType.INSTANT },
            { SchemaType.LOCAL_DATE },
            { SchemaType.LOCAL_TIME },
            { SchemaType.LOCAL_DATE_TIME },
        };
    }

    @DataProvider(name = "clientSchemas")
    public static Object[][] clientSchemas() {
        return new Object[][] {
            { SchemaType.AUTO_CONSUME },
            { SchemaType.AUTO_PUBLISH },
            { SchemaType.AUTO },
        };
    }

    @DataProvider(name = "structSchemas")
    public static Object[][] structSchemas() {
        return new Object[][] {
            { SchemaType.AVRO },
            { SchemaType.JSON },
            { SchemaType.PROTOBUF },
        };
    }

    @Test(dataProvider = "primitiveSchemas")
    public void testPrimitiveValidatorSuccess(SchemaType type) throws Exception {
        SchemaData data = SchemaData.builder()
            .type(type)
            .data(new byte[0])
            .build();
        SchemaDataValidator.validateSchemaData(data);
    }

    @Test(dataProvider = "primitiveSchemas", expectedExceptions = InvalidSchemaDataException.class)
    public void testPrimitiveValidatorInvalid(SchemaType type) throws Exception {
        SchemaData data = SchemaData.builder()
            .type(type)
            .data(new byte[10])
            .build();
        SchemaDataValidator.validateSchemaData(data);
    }

    @Test(dataProvider = "clientSchemas", expectedExceptions = InvalidSchemaDataException.class)
    public void testValidateClientSchemas(SchemaType type) throws Exception {
        SchemaData data = SchemaData.builder()
            .type(type)
            .data(new byte[0])
            .build();
        SchemaDataValidator.validateSchemaData(data);
    }

    @Test(dataProvider = "structSchemas")
    public void testStructValidatorSuccess(SchemaType type) throws Exception {
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        SchemaData data = SchemaData.builder()
            .type(type)
            .data(schema.getSchemaInfo().getSchema())
            .build();
        SchemaDataValidator.validateSchemaData(data);
    }

    @Test(dataProvider = "structSchemas", expectedExceptions = InvalidSchemaDataException.class)
    public void testStructValidatorInvalid(SchemaType type) throws Exception {
        SchemaData data = SchemaData.builder()
            .type(type)
            .data("bad-schema".getBytes(UTF_8))
            .build();
        SchemaDataValidator.validateSchemaData(data);
    }

    @Test
    public void testJsonSchemaTypeWithJsonSchemaData() throws Exception {
        ObjectMapper mapper = ObjectMapperFactory.getMapper().getObjectMapper();
        SchemaData data = SchemaData.builder()
            .type(SchemaType.JSON)
            .data(
                mapper.writeValueAsBytes(
                    new JsonSchemaGenerator(mapper)
                    .generateSchema(Foo.class)))
            .build();
        SchemaDataValidator.validateSchemaData(data);
    }

}
