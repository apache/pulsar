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
package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class BaseAvroSchemaCompatibilityTest {

    SchemaCompatibilityCheck schemaCompatibilityCheck;

    private static final String schemaJson1 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest$\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData1 = getSchemaData(schemaJson1);

    private static final String schemaJson2 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest$\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
                    "{\"name\":\"field2\",\"type\":\"string\",\"default\":\"foo\"}]}";
    private static final SchemaData schemaData2 = getSchemaData(schemaJson2);

    private static final String schemaJson3 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org" +
                    ".apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheckTest$\"," +
                    "\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData3 = getSchemaData(schemaJson3);

    private static final String schemaJson4 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest$\",\"fields\":[{\"name\":\"field1_v2\",\"type\":\"string\"," +
                    "\"aliases\":[\"field1\"]}]}";
    private static final SchemaData schemaData4 = getSchemaData(schemaJson4);

    private static final String schemaJson5 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest$\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\"," +
                    "\"string\"]}]}";
    private static final SchemaData schemaData5 = getSchemaData(schemaJson5);

    private static final String schemaJson6 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest$\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\"," +
                    "\"string\",\"int\"]}]}";
    private static final SchemaData schemaData6 = getSchemaData(schemaJson6);

    private static final String schemaJson7 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest$\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
                    "{\"name\":\"field2\",\"type\":\"string\",\"default\":\"foo\"},{\"name\":\"field3\"," +
                    "\"type\":\"string\",\"default\":\"bar\"}]}";
    private static final SchemaData schemaData7 = getSchemaData(schemaJson7);


    public abstract SchemaCompatibilityCheck getBackwardsCompatibleSchemaCheck();

    public abstract SchemaCompatibilityCheck getForwardCompatibleSchemaCheck();

    public abstract SchemaCompatibilityCheck getFullCompatibleSchemaCheck();


    /**
     * make sure new schema is backwards compatible with latest
     */
    @Test
    public void testBackwardCompatibility() {

        SchemaCompatibilityCheck schemaCompatibilityCheck = getBackwardsCompatibleSchemaCheck();
        // adding a field with default is backwards compatible
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData2),
                "adding a field with default is backwards compatible");
        // adding a field without default is NOT backwards compatible
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData3),
                "adding a field without default is NOT backwards compatible");
        // Modifying a field name is not backwards compatible
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData4),
                "Modifying a field name is not backwards compatible");
        // evolving field to a union is backwards compatible
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData5),
                "evolving field to a union is backwards compatible");
        // removing a field from a union is NOT backwards compatible
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData5, schemaData1),
                "removing a field from a union is NOT backwards compatible");
        // adding a field to a union is backwards compatible
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData5, schemaData6),
                "adding a field to a union is backwards compatible");
        // removing a field a union is NOT backwards compatible
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData6, schemaData5),
                "removing a field a union is NOT backwards compatible");
    }

    /**
     * Check to make sure the last schema version is forward-compatible with new schemas
     */
    @Test
    public void testForwardCompatibility() {

        SchemaCompatibilityCheck schemaCompatibilityCheck = getForwardCompatibleSchemaCheck();

        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData2),
                "adding a field is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData3),
                "adding a field is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData2, schemaData3),
                "adding a field is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData3, schemaData2),
                "adding a field is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData3, schemaData2),
                "adding a field is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData2, schemaData7),
                "removing fields is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData2, schemaData1),
                "removing fields with defaults forward compatible");
    }

    /**
     * Make sure the new schema is forward- and backward-compatible from the latest to newest and from the newest to latest.
     */
    @Test
    public void testFullCompatibility() {
        SchemaCompatibilityCheck schemaCompatibilityCheck = getFullCompatibleSchemaCheck();
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData2),
                "adding a field with default fully compatible");
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData1, schemaData3),
                "adding a field without default is not fully compatible");
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData3, schemaData1),
                "adding a field without default is not fully compatible");

    }

    private static SchemaData getSchemaData(String schemaJson) {
        return SchemaData.builder().data(schemaJson.getBytes()).type(SchemaType.AVRO).build();
    }
}
