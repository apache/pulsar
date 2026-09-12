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
package org.apache.pulsar.testclient;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class PerformanceConsumerArgsTest {

    private static PerformanceConsumer parse(String... args) {
        PerformanceConsumer consumer = new PerformanceConsumer();
        new CommandLine(consumer).parseArgs(args);
        return consumer;
    }

    @Test
    public void testScalableConsumerTypeDefaultsToQueue() {
        PerformanceConsumer consumer = parse("my-topic");
        assertEquals(consumer.scalableConsumerType,
                PerformanceConsumer.ScalableConsumerType.Queue);
    }

    @Test
    public void testScalableConsumerTypeStreamLongOption() {
        PerformanceConsumer consumer = parse("--scalable-consumer-type", "Stream", "my-topic");
        assertEquals(consumer.scalableConsumerType,
                PerformanceConsumer.ScalableConsumerType.Stream);
    }

    @Test
    public void testScalableConsumerTypeShortOption() {
        PerformanceConsumer consumer = parse("-sct", "Queue", "my-topic");
        assertEquals(consumer.scalableConsumerType,
                PerformanceConsumer.ScalableConsumerType.Queue);
    }
}
