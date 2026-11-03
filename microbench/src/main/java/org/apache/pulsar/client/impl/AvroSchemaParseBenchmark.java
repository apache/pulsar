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

package org.apache.pulsar.client.impl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.util.SchemaUtil;
import org.apache.pulsar.common.schema.AvroSchemaCompat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 3, time = 3)
public class AvroSchemaParseBenchmark {
    public static class Address {
        public String street;
        public String city;
        public int zip;
    }

    public static class Order {
        public long id;
        public String product;
        public double price;
        public List<String> tags;
    }
    public static class Person {
        public String name;
        public int age;
        public boolean active;
        public Address address;
        public List<Order> orders;
        public Map<String, String> attributes;
    }
    String json;

    @Setup
    public void setup() {
        json = new String(AvroSchema.of(Person.class).getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
    }

    @Benchmark
    public org.apache.avro.Schema parseAvroSchema()  { return SchemaUtil.parseAvroSchema(json); }
    @Benchmark public String normalizeOnly()                    { return AvroSchemaCompat.normalizeNamedTypeReferences(json); }
    @Benchmark public Object schemaOf()                         { return AvroSchema.of(Person.class); }
}