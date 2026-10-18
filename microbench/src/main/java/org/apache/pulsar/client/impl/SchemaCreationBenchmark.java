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

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class SchemaCreationBenchmark {
    public static class Foo {
        public String name;
        public int age;
        public java.util.List<String> tags;
    }
    private Schema<Foo> prebuilt;
    private SchemaDefinition<Foo> definition;
    private PulsarClientImplementationBindingImpl binding;
    @Setup(Level.Trial)
    public void setup() {
        binding = new PulsarClientImplementationBindingImpl();
        definition = SchemaDefinition.<Foo>builder().withPojo(Foo.class).build();
        prebuilt = Schema.AVRO(Foo.class);   // warms cache, keeps value strongly reachable
    }
    @Benchmark
    public void cacheMiss(Blackhole bh) {
        binding.clearAvroCache();
        bh.consume(binding.newAvroSchema(Foo.class));
    }
    @Benchmark
    public void cacheHitAvroSchema(Blackhole bh) {
        bh.consume(binding.newAvroSchema(Foo.class));
    }
    @Benchmark
    public void cloneOnly(Blackhole bh) {
        bh.consume(prebuilt.clone());
    }

    @Benchmark
    public void ofOnly(Blackhole bh) {
        binding.clearAvroCache();
        bh.consume(AvroSchema.of(Foo.class));
    }
    @Benchmark
    public void clearOnly() {
        binding.clearAvroCache();
    }


}
