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
package org.apache.pulsar.sql.presto;

import com.facebook.presto.spi.type.BigintType;
import io.netty.buffer.ByteBufAllocator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.StructSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class TestJSONSchemaHandler {

    @Data
    public static class Foo1 {
        String field1;

        Bar bar;
    }

    @Data static class Bar {
        String field1;

        String field2;
    }

    @Test
    public void testJsonSchemaHandler() throws IOException {
        List<PulsarColumnHandle> columnHandles = new ArrayList();
        RawMessage message = mock(RawMessage.class);
        TopicName topicName = mock(TopicName.class);
        PulsarConnectorConfig pulsarConnectorConfig = mock(PulsarConnectorConfig.class);
        JSONSchemaHandler jsonSchemaHandler = new JSONSchemaHandler(topicName, pulsarConnectorConfig,
                StructSchema.parseSchemaInfo(SchemaDefinition.builder().withPojo(Foo1.class).build(), SchemaType.JSON), columnHandles);
        JSONSchema jsonSchema = JSONSchema.of(Foo1.class);

        Foo1 foo1 = new Foo1();
        foo1.setField1("value1");
        foo1.setBar(new Bar());
        foo1.getBar().setField1("value1");
        byte[] bytes = jsonSchema.encode(foo1);
        when(message.getData()).thenReturn(ByteBufAllocator.DEFAULT
                .buffer(bytes.length, bytes.length).writeBytes(bytes));
        jsonSchemaHandler.deserialize(message);

        Object object  = ((GenericJsonRecord)jsonSchemaHandler.deserialize(message)).getField("field1");
        Assert.assertEquals(foo1.field1, (String)object);
        PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle("test",
                "bar.field1",
                BigintType.BIGINT,
                true,
                true,
                new String[5],
                new Integer[5]);
        columnHandles.add(pulsarColumnHandle);
        object = jsonSchemaHandler.extractField(0, jsonSchemaHandler.deserialize(message));
        Assert.assertEquals(foo1.bar.field1, (String)object);
    }
}