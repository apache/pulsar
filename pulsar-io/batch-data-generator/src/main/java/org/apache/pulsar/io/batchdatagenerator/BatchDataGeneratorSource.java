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
package org.apache.pulsar.io.batchdatagenerator;

import io.codearte.jfairy.Fairy;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.SourceContext;

@Slf4j
public class BatchDataGeneratorSource implements BatchSource<Person> {

    private Fairy fairy;
    private SourceContext sourceContext;
    int iteration;
    int maxRecordsPerCycle = 10;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) {
       this.fairy = Fairy.create();
       this.sourceContext = sourceContext;
    }

    @Override
    public void discover(Consumer<byte[]> taskEater) {
        log.info("Generating one task for each instance");
        for (int i = 0; i < sourceContext.getNumInstances(); ++i) {
            taskEater.accept("something".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void prepare(byte[] instanceSplit) {
        log.info("Instance " + sourceContext.getInstanceId() + " got a new discovered task");
        final String str = new String(instanceSplit, StandardCharsets.UTF_8);
        final String expected = "something";
        assert str.equals(expected);
        iteration = 0;
    }

    @Override
    public Record<Person> readNext() throws Exception {
        if (iteration++ < maxRecordsPerCycle) {
            Thread.sleep(50);
            return new Record<Person>() {
                @Override
                public Optional<String> getKey() {
                    return Optional.empty();
                }

                @Override
                public Person getValue() {
                    return new Person(fairy.person());
                }
            };
        }
        return null;
    }

    @Override
    public void close() {

    }
}
