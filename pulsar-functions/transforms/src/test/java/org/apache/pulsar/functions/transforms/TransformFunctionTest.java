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
package org.apache.pulsar.functions.transforms;

import static org.apache.pulsar.functions.transforms.Utils.createTestAvroKeyValueRecord;
import static org.apache.pulsar.functions.transforms.Utils.getRecord;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertNull;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TransformFunctionTest {

    @DataProvider(name = "validConfigs")
    public static Object[][] validConfigs() {
        return new Object[][] {
                {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field'}]}"},
                {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 'key'}]}"},
                {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 'value'}]}"},
                {"{'steps': [{'type': 'unwrap-key-value'}]}"},
                {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': false}]}"},
                {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': true}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 'STRING'}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 'key'}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 'value'}]}"},
        };
    }

    @Test(dataProvider = "validConfigs")
    void testValidConfig(String validConfig) {
        String userConfig = validConfig.replace("'", "\"");
        Map<String, Object> config = new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        Context context = new Utils.TestContext(null, config);
        TransformFunction transformFunction = new TransformFunction();
        transformFunction.initialize(context);
    }

    @DataProvider(name = "invalidConfigs")
    public static Object[][] invalidConfigs() {
        return new Object[][] {
                {"{}"},
                {"{'steps': 'invalid'}"},
                {"{'steps': [{}]}"},
                {"{'steps': [{'type': 'invalid'}]}"},
                {"{'steps': [{'type': 'drop-fields'}]}"},
                {"{'steps': [{'type': 'drop-fields', 'fields': ''}]}"},
                {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 'invalid'}]}"},
                {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 42}]}"},
                {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': 'invalid'}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 42}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 'INVALID'}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 'invalid'}]}"},
                {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 42}]}"},
        };
    }

    @Test(dataProvider = "invalidConfigs")
    void testInvalidConfig(String invalidConfig) {
        String userConfig = invalidConfig.replace("'", "\"");
        Map<String, Object> config = new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        Context context = new Utils.TestContext(null, config);
        TransformFunction transformFunction = new TransformFunction();
        assertThrows(IllegalArgumentException.class, () -> transformFunction.initialize(context));
    }

    @Test
    void testDropFields() throws Exception {
        String userConfig = (""
                + "{'steps': ["
                + "    {'type': 'drop-fields', 'fields': 'keyField1'},"
                + "    {'type': 'drop-fields', 'fields': 'keyField2', 'part': 'key'},"
                + "    {'type': 'drop-fields', 'fields': 'keyField3', 'part': 'value'},"
                + "    {'type': 'drop-fields', 'fields': 'valueField1'},"
                + "    {'type': 'drop-fields', 'fields': 'valueField2', 'part': 'key'},"
                + "    {'type': 'drop-fields', 'fields': 'valueField3', 'part': 'value'}"
                + "]}").replace("'", "\"");
        Map<String, Object> config = new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        TransformFunction transformFunction = new TransformFunction();

        Record<GenericObject> record = createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, config);
        transformFunction.initialize(context);
        transformFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        GenericData.Record keyAvroRecord = getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));
        assertNull(keyAvroRecord.getSchema().getField("keyField1"));
        assertNull(keyAvroRecord.getSchema().getField("keyField2"));

        GenericData.Record valueAvroRecord =
                getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        assertEquals(valueAvroRecord.get("valueField2"), new Utf8("value2"));
        assertNull(valueAvroRecord.getSchema().getField("valueField1"));
        assertNull(valueAvroRecord.getSchema().getField("valueField3"));
    }

    // TODO: just for demo. To be removed
    @Test
    void testRemoveMergeAndToString() throws Exception {
        String userConfig = (""
                + "{'steps': ["
                + "    {'type': 'drop-fields', 'fields': 'keyField1'},"
                + "    {'type': 'merge-key-value'},"
                + "    {'type': 'unwrap-key-value'},"
                + "    {'type': 'cast', 'schema-type': 'STRING'}"
                + "]}").replace("'", "\"");
        Map<String, Object> config = new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        TransformFunction transformFunction = new TransformFunction();

        Record<GenericObject> record = createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, config);
        transformFunction.initialize(context);
        transformFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        assertEquals(message.getValue(), "{\"keyField2\": \"key2\", \"keyField3\": \"key3\", \"valueField1\": "
                + "\"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}");
    }

}
