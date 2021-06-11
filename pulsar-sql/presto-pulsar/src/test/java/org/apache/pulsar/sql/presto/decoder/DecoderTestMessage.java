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
package org.apache.pulsar.sql.presto.decoder;

import lombok.Data;

import java.util.List;
import java.util.Map;

public class DecoderTestMessage {

    public static enum TestEnum {
        TEST_ENUM_1,
        TEST_ENUM_2,
        TEST_ENUM_3
    }

    public int intField;
    public String stringField;
    public float floatField;
    public double doubleField;
    public boolean booleanField;
    public long longField;
    @org.apache.avro.reflect.AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
    public long timestampField;
    @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"time-millis\" }")
    public int timeField;
    @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
    public int dateField;
    public TestRow rowField;
    public TestEnum enumField;

    public List<String> arrayField;
    public Map<String, Long> mapField;
    public CompositeRow compositeRow;

    public static class TestRow {
        public String stringField;
        public int intField;
        public NestedRow nestedRow;
    }


    public static class NestedRow {
        public String stringField;
        public long longField;
    }


    public static class CompositeRow {
        public String stringField;
        public List<NestedRow> arrayField;
        public Map<String, NestedRow> mapField;
        public NestedRow nestedRow;
        public Map<String,List<Long>> structedField;
    }

    /**
     * POJO for cyclic detect.
     */
    @Data
    public static class CyclicFoo {
        private String field1;
        private Integer field2;
        private CyclicBoo boo;
    }

    @Data
    public static class CyclicBoo {
        private String field1;
        private Boolean field2;
        private CyclicFoo foo;
    }

}
