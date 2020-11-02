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
package org.apache.pulsar.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.broker.admin.AdminApiSchemaTest;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonReader;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class SimpleTextOutputStreamTest {

    private ByteBuf buf;
    private SimpleTextOutputStream stream;

    @Data
    public static class Foo {
        @Nullable
        private String field1;
        @Nullable
        private String field2;
        private int field3;
        @Nullable
        private Bar field4;
        @Nullable
        private Color color;
        @AvroDefault("\"defaultValue\"")
        private String fieldUnableNull;
    }

    @Data
    public static class Bar {
        private boolean field1;
    }

    public enum  Color {
        RED,
        BLUE
    }

    @BeforeMethod
    public void reset() {
        buf = Unpooled.buffer(4096);
        stream = new StatsOutputStream(buf);
    }

    @Test
    public void test() throws Exception {
        StringSchema ss = new StringSchema();
        System.out.println(ss.getSchemaInfo().getType());
        System.out.println(ObjectMapperFactory.getThreadLocal().writeValueAsString(ss));
        String data = new String("{\"type\":\"STRING\", \"schema\":\"\"}".getBytes());
        SchemaInfo si = ObjectMapperFactory.getThreadLocal().readValue(data, SchemaInfo.class);
        System.out.println(si.getType());
    }

    @Test
    public void test2() throws Exception {
        Schema<Foo> encodeSchema = Schema.JSON(Foo.class);
        GenericSchema decodeSchema = GenericJsonSchema.of(encodeSchema.getSchemaInfo());
        String schemaStr = new String(decodeSchema.getSchemaInfo().getSchema());
        System.out.println(schemaStr);
        System.out.println(ObjectMapperFactory.getThreadLocal().writeValueAsString(decodeSchema.getSchemaInfo()));
        String data = new String("{\"type\":\"JSON\", \"schema\":\"eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IkZvbyIsIm5hbWVzcGFjZSI6Im9yZy5hcGFjaGUucHVsc2FyLnV0aWxzLlNpbXBsZVRleHRPdXRwdXRTdHJlYW" +
                "1UZXN0IiwiZmllbGRzIjpbeyJuYW1lIjoiZmllbGQxIiwidHlwZSI6WyJudWxsIiwic3RyaW5nIl0sImRlZmF1bHQiOm51bGx9LHsibmFtZSI6ImZpZWxkMiIsInR5cGUiOlsibnVsbCIsInN0cmluZyJdLCJkZWZhdWx0IjpudWx" +
                "sfSx7Im5hbWUiOiJmaWVsZDMiLCJ0eXBlIjoiaW50In0seyJuYW1lIjoiZmllbGQ0IiwidHlwZSI6WyJudWxsIix7InR5cGUiOiJyZWNvcmQiLCJuYW1lIjoiQmFyIiwiZmllbGRzIjpbeyJuYW1lIjoiZmllbGQxIiwidHlwZSI6" +
                "ImJvb2xlYW4ifV19XSwiZGVmYXVsdCI6bnVsbH0seyJuYW1lIjoiY29sb3IiLCJ0eXBlIjpbIm51bGwiLHsidHlwZSI6ImVudW0iLCJuYW1lIjoiQ29sb3IiLCJzeW1ib2xzIjpbIlJFRCIsIkJMVUUiXX1dLCJkZWZhdWx0Ijpud" +
                "WxsfSx7Im5hbWUiOiJmaWVsZFVuYWJsZU51bGwiLCJ0eXBlIjpbIm51bGwiLCJzdHJpbmciXSwiZGVmYXVsdCI6ImRlZmF1bHRWYWx1ZSJ9XX0=\"}");
        System.out.println(new String(Base64.getEncoder().encode(data.getBytes())));
        SchemaInfo si = ObjectMapperFactory.getThreadLocal().readValue(data, SchemaInfo.class);
        Schema s = AutoConsumeSchema.getSchema(si);
        System.out.println(new String(s.getSchemaInfo().getSchema()));

        GenericJsonReader reader = new GenericJsonReader(Collections.emptyList(), si);
        String jsonStr = "{\"field1\":\"123455\", \"field2\":\"abcdefg\", \"field3\":2, \"field4\": {\"field1\":false}, \"color\":\"BLUE\"}";
        String payload = new  String(Base64.getEncoder().encode(s.encode(reader.read(jsonStr.getBytes()))));
        System.out.println(payload);

        JsonNode jn = ObjectMapperFactory.getThreadLocal().readTree(jsonStr);
        System.out.println(jn.toString());
        System.out.println(((GenericJsonRecord)s.decode(Base64.getDecoder().decode(payload.getBytes()))).getJsonNode());
    }

    @Test
    public void testBooleanFormat() {
        stream.write(false);
        assertEquals(str(), "false");

        stream.write(true);
        assertEquals(str(), "true");
    }

    @Test
    public void testLongFormat() {
        stream.write(0);
        assertEquals(str(), "0");

        stream.write(-1);
        assertEquals(str(), "-1");

        stream.write(123456789);
        assertEquals(str(), "123456789");

        stream.write(-123456789);
        assertEquals(str(), "-123456789");

        long i = 2 * (long) Integer.MAX_VALUE;

        stream.write(i);
        assertEquals(str(), Long.toString(i));

        stream.write(Long.MAX_VALUE);
        assertEquals(str(), Long.toString(Long.MAX_VALUE));

        stream.write(Long.MIN_VALUE);
        assertEquals(str(), Long.toString(Long.MIN_VALUE));

        // Testing trailing 0s
        stream.write(100);
        assertEquals(str(), "100");

        stream.write(-1000);
        assertEquals(str(), "-1000");
    }

    @Test
    public void testDoubleFormat() {
        stream.write(0.0);
        assertEquals(str(), "0.0");

        stream.write(1.0);
        assertEquals(str(), "1.0");

        stream.write(1.123456789);
        assertEquals(str(), "1.123");

        stream.write(123456.123456789);
        assertEquals(str(), "123456.123");

        stream.write(-123456.123456789);
        assertEquals(str(), "-123456.123");

        stream.write(-123456.003456789);
        assertEquals(str(), "-123456.003");

        stream.write(-123456.100456789);
        assertEquals(str(), "-123456.100");
    }

    @Test
    public void testString() {
        stream.writeEncoded("�\b`~�ýý8ýH\\abcd\"");
        assertEquals(str(), "\\ufffd\\u0008`~\\ufffd\\u00fd\\u00fd8\\u00fdH\\\\abcd\\\"");
    }

    public String str() {
        String s = buf.toString(StandardCharsets.UTF_8);
        reset();
        return s;
    }
}
