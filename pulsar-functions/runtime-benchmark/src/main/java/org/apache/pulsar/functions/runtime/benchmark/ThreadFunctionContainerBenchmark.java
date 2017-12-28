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
package org.apache.pulsar.functions.runtime.benchmark;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.functions.api.examples.ExclamationFunction;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.FunctionID;
import org.apache.pulsar.functions.runtime.InstanceID;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.apache.pulsar.functions.runtime.container.ThreadFunctionContainerFactory;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.serde.Utf8StringSerDe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark {@link org.apache.pulsar.functions.runtime.container.ThreadFunctionContainer}.
 */
@BenchmarkMode({ Mode.Throughput })
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ThreadFunctionContainerBenchmark {

    private static final Logger log = LoggerFactory.getLogger(ThreadFunctionContainerBenchmark.class);

    @Param({ "-1", "60000" })
    int timeBudgetInMs;

    byte[] data;
    FunctionContainer container;

    @Setup
    public void prepare() {
        this.data = Utf8StringSerDe.of().serialize("test-data");
        ThreadFunctionContainerFactory factory =
            new ThreadFunctionContainerFactory(1024);
        FunctionConfig fnConfig = new FunctionConfig();
        fnConfig.setName("benchmark");
        fnConfig.setClassName(ExclamationFunction.class.getName());
        fnConfig.setInputSerdeClassName(Utf8StringSerDe.class.getName());
        fnConfig.setOutputSerdeClassName(Utf8StringSerDe.class.getName());
        fnConfig.setSinkTopic("test-sink");
        fnConfig.setSourceTopic("test-source");
        fnConfig.setTenant("test-tenant");
        fnConfig.setNamespace("test-namespace");

        JavaInstanceConfig config =
            new JavaInstanceConfig();
        config.setTimeBudgetInMs(timeBudgetInMs);
        config.setMaxMemory(1024);
        config.setFunctionId(new FunctionID());
        config.setFunctionVersion(UUID.randomUUID().toString());
        config.setInstanceId(new InstanceID());
        config.setFunctionConfig(fnConfig);
        container = factory.createContainer(config, "test-jar");
        try {
            container.start();
        } catch (Exception e) {
            log.error("Failed to start container", e);
        }
    }

    @TearDown
    public void teardown() {
        if (null != container) {
            container.stop();
        }
    }

    @Benchmark
    public void testSendMessageAsync() {
        container.sendMessage(
            "test-source",
            "test-message-id",
            data);
    }

    @Benchmark
    public void testSendMessage() {
        try {
            container.sendMessage(
                "test-source",
                "test-message-id",
                data).get();
        } catch (InterruptedException | ExecutionException e) {
            log.info("Failed to process messages", e);
        }
    }

}
