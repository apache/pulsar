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
package org.apache.pulsar.common.util;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares hashing a string key directly as UTF-8 with encoding it to a byte array first, as Key_Shared dispatch
 * did for a plain string partition key. Run it with {@code -prof gc}: the direct hash of an ASCII key doesn't
 * allocate.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
// java.lang open, as bin/pulsar runs the broker: without it, the direct hash encodes the key
@Fork(value = 2, jvmArgsAppend = "--add-opens=java.base/java.lang=ALL-UNNAMED")
public class Murmur3Utf8HashBenchmark {
    @Param({"device-123456", "3f2b8c1e-5a7d-4e9b-8c6f-1d2e3f4a5b6c", "käyttäjä-123456"})
    private String key;
    private byte[] keyBytes;

    @Setup
    public void setup() {
        keyBytes = key.getBytes(StandardCharsets.UTF_8);
    }

    @Benchmark
    public int stringToBytesThenHash() {
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes(StandardCharsets.UTF_8));
    }

    @Benchmark
    public int hashStringDirectly() {
        return Murmur3_32Hash.makeHashUtf8(key);
    }

    @Benchmark
    public int hashExistingBytes() {
        return Murmur3_32Hash.getInstance().makeHash(keyBytes);
    }
}
