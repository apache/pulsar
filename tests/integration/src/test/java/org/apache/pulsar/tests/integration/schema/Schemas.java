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
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.tests.integration.schema;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.avro.reflect.AvroDefault;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.math.BigDecimal;

/**
 * Keep a list of schemas for testing.
 */
public final class Schemas {

    /**
     * A Person Struct.
     */
    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    public static class Person {

        private String name;
        private int age;

    }

    /**
     * A Person Struct.
     */
    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    public static class PersonConsumeSchema {

        private String name;
        private int age;
        @AvroDefault("\"male\"")
        private String gender;

    }

    /**
     * A Student Struct.
     */
    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    public static class Student {

        private String name;
        private int age;
        private int gpa;
        private int grade;

    }

    @Data
    @ToString
    @EqualsAndHashCode
    @Builder
    public static class AvroLogicalType{
        @org.apache.avro.reflect.AvroSchema("{\n" +
                "  \"type\": \"bytes\",\n" +
                "  \"logicalType\": \"decimal\",\n" +
                "  \"precision\": 4,\n" +
                "  \"scale\": 2\n" +
                "}")
        BigDecimal decimal;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"date\"}")
        LocalDate date;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
        DateTime timestampMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"time-millis\"}")
        LocalTime timeMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        long timestampMicros;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"time-micros\"}")
        long timeMicros;
    }

    private Schemas() {}

}
