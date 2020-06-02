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

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;

/**
 * Utils for testing avro.
 */
public class SchemaTestUtils {

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
    public static class FooV2 {
        @Nullable
        private String field1;
        private int field3;
    }

    @Data
    public static class Bar {
        private boolean field1;
    }

    @Data
    public static class NestedBar {
        private boolean field1;
        private Bar nested;
    }

    @Data
    public static class NestedBarList {
        private boolean field1;
        private List<Bar> list;
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static class DerivedFoo extends Foo {
        private String field5;
        private int field6;
        private Foo foo;
    }

    public enum  Color {
        RED,
        BLUE
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static class DerivedDerivedFoo extends DerivedFoo {
        private String field7;
        private int field8;
        private DerivedFoo derivedFoo;
        private Foo foo2;
    }

    public static final String SCHEMA_AVRO_NOT_ALLOW_NULL
            = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",\"string\"]," +
            "\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\",{\"type\":" +
            "\"record\",\"name\":\"Bar\",\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\"," +
            "\"symbols\":[\"RED\",\"BLUE\"]}],\"default\":null},{\"name\":\"fieldUnableNull\",\"type\":\"string\",\"default\":\"defaultValue\"}]}";

    public static final String SCHEMA_AVRO_ALLOW_NULL = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":[{\"name\":\"field1\"," +
            "\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"" +
            "null\",{\"type\":\"record\",\"name\":\"Bar\",\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\"" +
            ",\"symbols\":[\"RED\",\"BLUE\"]}],\"default\":null},{\"name\":\"fieldUnableNull\",\"type\":[\"null\",\"string\"],\"default\":\"defaultValue\"}]}";

    public static final String SCHEMA_JSON_NOT_ALLOW_NULL
            = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\"" +
            ":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Bar\",\"fields\":[{\"name\":\"" +
            "field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}],\"default\":null},{\"name\":\"fieldUnableNull\"," +
            "\"type\":\"string\",\"default\":\"defaultValue\"}]}";
    public static final String SCHEMA_JSON_ALLOW_NULL
            = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Bar\",\"fields\":" +
            "[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}],\"default\":null},{\"name\":" +
            "\"fieldUnableNull\",\"type\":[\"null\",\"string\"],\"default\":\"defaultValue\"}]}";
    public static final String KEY_VALUE_SCHEMA_INFO_INCLUDE_PRIMITIVE = "{\"key\":{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":[{\"name\":\"" +
            "field1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\"," +
            "{\"type\":\"record\",\"name\":\"Bar\",\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\"" +
            ",\"BLUE\"]}],\"default\":null},{\"name\":\"fieldUnableNull\",\"type\":[\"null\",\"string\"],\"default\":\"defaultValue\"}]},\"value\":\"\"}";
    public static final String KEY_VALUE_SCHEMA_INFO_NOT_INCLUDE_PRIMITIVE = "{\"key\":{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":[{\"name\":\"field1\"" +
            ",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\",{\"type\":\"record\"" +
            ",\"name\":\"Bar\",\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}],\"default\":null}," +
            "{\"name\":\"fieldUnableNull\",\"type\":[\"null\",\"string\"],\"default\":\"defaultValue\"}]},\"value\":{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\"fields\":" +
            "[{\"name\":\"field1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\"," +
            "{\"type\":\"record\",\"name\":\"Bar\",\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}]," +
            "\"default\":null},{\"name\":\"fieldUnableNull\",\"type\":[\"null\",\"string\"],\"default\":\"defaultValue\"}]}}";

    public static String[] FOO_FIELDS = {
            "field1",
            "field2",
            "field3",
            "field4",
            "color",
            "fieldUnableNull"
    };

    public static String TEST_MULTI_VERSION_SCHEMA_STRING = "TEST";

    public static String TEST_MULTI_VERSION_SCHEMA_DEFAULT_STRING = "defaultValue";

}
