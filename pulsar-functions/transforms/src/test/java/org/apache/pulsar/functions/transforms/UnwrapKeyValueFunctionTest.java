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
import static org.testng.Assert.*;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class UnwrapKeyValueFunctionTest {

    @Test
    void testInvalidConfig() {
        Map<String, Object> config = ImmutableMap.of("unwrapKey", new ArrayList<>());
        Utils.TestContext context = new Utils.TestContext(createTestAvroKeyValueRecord(), config);

        UnwrapKeyValueFunction function = new UnwrapKeyValueFunction();
        assertThrows(IllegalArgumentException.class, () -> function.initialize(context));

    }

    @Test
    void testKeyValueUnwrapValue() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());

        UnwrapKeyValueFunction function = new UnwrapKeyValueFunction();
        function.initialize(context);
        function.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();

        GenericData.Record read = getRecord(message.getSchema(), (byte[]) message.getValue());
        assertEquals(read.toString(), "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": "
                + "\"value3\"}");
    }

    @Test
    void testKeyValueUnwrapKey() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        HashMap<String, Object> config = new HashMap<>();
        config.put("unwrapKey", true);
        Utils.TestContext context = new Utils.TestContext(record, config);

        UnwrapKeyValueFunction function = new UnwrapKeyValueFunction();
        function.initialize(context);
        function.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();

        GenericData.Record read = getRecord(message.getSchema(), (byte[]) message.getValue());
        assertEquals(read.toString(), "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}");
    }

    @Test
    void testPrimitive() throws Exception {
        Record<String> record = new Utils.TestRecord<>(Schema.STRING, "test-message", "test-key");
        Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());

        UnwrapKeyValueFunction function = new UnwrapKeyValueFunction();
        function.initialize(context);
        function.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.STRING, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();

        assertEquals(message.getSchema(), record.getSchema());
        assertEquals(message.getValue(), record.getValue());
        assertEquals(message.getKey(), record.getKey().orElse(null));
    }
}