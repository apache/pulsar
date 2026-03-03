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
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import org.apache.avro.NameValidator;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.broker.service.schema.proto.DataRecordOuterClass;
import org.apache.pulsar.broker.service.schema.validator.StructSchemaDataValidator.CompatibleNameValidator;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
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
        ObjectReader reader = ObjectMapperFactory.getMapper().reader().forType(JsonSchema.class);
        try {
            org.apache.avro.Schema.Parser avroSchemaParser = new org.apache.avro.Schema.Parser();
            avroSchemaParser.setValidateDefaults(false);
            org.apache.avro.Schema schema = avroSchemaParser.parse(new String(data.getData(), UTF_8));
        } catch (Exception e) {
            try {
                reader.readValue(data.getData());
            } catch (Exception ioe) {
                throw new InvalidSchemaDataException("Invalid schema definition data for "
                        + data.getType() + " schema", ioe);
            }
        }
    }

    @Test
    public void testCompatibleNameValidatorValidNames() {
        CompatibleNameValidator validator = new CompatibleNameValidator();

        String[] validNames = {
            "validName",
            "ValidName",
            "valid_name",
            "valid$name",
            "_validName",
            "$validName",
            "name123",
            "Name_123$",
            "a",
            "A",
            "_",
            "$",
            "validNameWithMultiple$ymbols_and_numbers123"
        };

        for (String name : validNames) {
            NameValidator.Result result = validator.validate(name);
            Assert.assertTrue(result.isOK(),
                "Expected validation to pass for name: '" + name + "', but got error: " + result.getErrors());
        }
    }

    @Test
    public void testCompatibleNameValidatorInvalidNames() {
        CompatibleNameValidator validator = new CompatibleNameValidator();

        String[] invalidNames = {
            null,
            "",
            "123name",
            "1name",
            "name-with-dash",
            "name with space",
            "name.with.dot",
            "name@symbol",
            "name#hash",
            "name%percent",
            "name&ampersand",
            "name*asterisk",
            "name(parentheses)",
            "name+plus",
            "name=equals",
            "name[brackets]",
            "name{braces}",
            "name|pipe",
            "name\\backslash",
            "name:colon",
            "name;semicolon",
            "name\"quote",
            "name'apostrophe",
            "name<greater>",
            "name,comma",
            "name?question",
            "name!exclamation",
            "name`backtick",
            "name~tilde",
            "name^caret"
        };

        for (String name : invalidNames) {
            NameValidator.Result result = validator.validate(name);
            Assert.assertFalse(result.isOK(), "Expected validation to fail for name: '" + name + "'");
        }
    }

    @Test
    public void testCompatibleNameValidatorSpecificErrorMessages() throws Exception {
        CompatibleNameValidator validator = new CompatibleNameValidator();

        NameValidator.Result nullResult = validator.validate(null);
        Assert.assertFalse(nullResult.isOK());
        Assert.assertEquals(nullResult.getErrors(), "Null name");

        NameValidator.Result emptyResult = validator.validate("");
        Assert.assertFalse(emptyResult.isOK());
        Assert.assertEquals(emptyResult.getErrors(), "Empty name");

        NameValidator.Result invalidFirstCharResult = validator.validate("123name");
        Assert.assertFalse(invalidFirstCharResult.isOK());
        Assert.assertTrue(invalidFirstCharResult.getErrors().contains("Illegal initial character"));

        NameValidator.Result invalidCharResult = validator.validate("name-with-dash");
        Assert.assertFalse(invalidCharResult.isOK());
        Assert.assertTrue(invalidCharResult.getErrors().contains("Illegal character in"));
    }

    @Test
    public void testCompatibleNameValidatorEdgeCases() throws Exception {
        CompatibleNameValidator validator = new CompatibleNameValidator();

        Assert.assertTrue(validator.validate("a").isOK());
        Assert.assertTrue(validator.validate("A").isOK());
        Assert.assertTrue(validator.validate("_").isOK());
        Assert.assertTrue(validator.validate("$").isOK());

        NameValidator.Result longNameResult = validator.validate("a".repeat(1000));
        Assert.assertTrue(longNameResult.isOK());

        NameValidator.Result nameWithOnlyDigits = validator.validate("123");
        Assert.assertFalse(nameWithOnlyDigits.isOK());
        Assert.assertTrue(nameWithOnlyDigits.getErrors().contains("Illegal initial character"));
    }

    @Test
    public void testAvroCompatible() throws InvalidSchemaDataException {
        final ProtobufSchema<DataRecordOuterClass.DataRecord> protobufSchema =
                ProtobufSchema.of(DataRecordOuterClass.DataRecord.class);
        StructSchemaDataValidator.of().validate(SchemaData.fromSchemaInfo(protobufSchema.getSchemaInfo()));
    }

}
