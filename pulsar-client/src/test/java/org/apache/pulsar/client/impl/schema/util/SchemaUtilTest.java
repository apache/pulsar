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
package org.apache.pulsar.client.impl.schema.util;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl.JSR310_CONVERSION_ENABLED;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SchemaUtilTest {

    @Test
    public void schemaWithoutJsr310EnabledPropertyReturnsFalse() {
        SchemaUtil.setGlobalJsr310ConversionEnabled(null);
        SchemaInfo schemaInfo = emptyPropertiesSchema();
        boolean isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
        assertFalse(isJsr310Enabled);
    }

    @Test
    public void schemaWithJsr310DisabledPropertyReturnsFalse() {
        SchemaUtil.setGlobalJsr310ConversionEnabled(null);
        SchemaInfo schemaInfo = disabledJsr310PropertiesSchema();
        boolean isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
        assertFalse(isJsr310Enabled);
    }

    @Test
    public void schemaWithJsr310EnabledPropertyReturnsTrue() {
        SchemaUtil.setGlobalJsr310ConversionEnabled(null);
        SchemaInfo schemaInfo = enabledJsr310PropertiesSchema();
        boolean isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
        assertTrue(isJsr310Enabled);
    }

    @Test
    public void globalJsr310DisabledAlwaysReturnsFalse() {
        SchemaUtil.setGlobalJsr310ConversionEnabled(false);
        boolean isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(emptyPropertiesSchema());
        assertFalse(isJsr310Enabled);
        isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(disabledJsr310PropertiesSchema());
        assertFalse(isJsr310Enabled);
        isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(enabledJsr310PropertiesSchema());
        assertFalse(isJsr310Enabled);
    }

    @Test
    public void globalJsr310EnabledAlwaysReturnsTrue() {
        SchemaUtil.setGlobalJsr310ConversionEnabled(true);
        boolean isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(emptyPropertiesSchema());
        assertTrue(isJsr310Enabled);
        isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(disabledJsr310PropertiesSchema());
        assertTrue(isJsr310Enabled);
        isJsr310Enabled = SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo(enabledJsr310PropertiesSchema());
        assertTrue(isJsr310Enabled);
    }

    private static SchemaInfo emptyPropertiesSchema() {
        return SchemaInfo.builder()
                .schema("{\"type\": \"string\"}".getBytes())
                .type(SchemaType.AVRO)
                .name("unitTest")
                .properties(new HashMap<>())
                .build();
    }

    private static SchemaInfo disabledJsr310PropertiesSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put(JSR310_CONVERSION_ENABLED, "false");

        return SchemaInfo.builder()
                .schema("{\"type\": \"string\"}".getBytes())
                .type(SchemaType.AVRO)
                .name("unitTest")
                .properties(properties)
                .build();
    }

    private static SchemaInfo enabledJsr310PropertiesSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put(JSR310_CONVERSION_ENABLED, "true");

        return SchemaInfo.builder()
                .schema("{\"type\": \"string\"}".getBytes())
                .type(SchemaType.AVRO)
                .name("unitTest")
                .properties(properties)
                .build();
    }

}
