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
package org.apache.pulsar.functions.utils;

import static org.testng.Assert.*;
import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

public class BatchingUtilsTest {

    @Test
    public void testConvert() {
        BatchingConfig config = BatchingConfig.builder()
                .enabled(true)
                .batchingMaxPublishDelayMs(30)
                .roundRobinRouterBatchingPartitionSwitchFrequency(10)
                .batchingMaxMessages(1000)
                .batchBuilder("DEFAULT")
                .build();
        Function.BatchingSpec spec = BatchingUtils.convert(config);
        assertEquals(spec.getEnabled(), true);
        assertEquals(spec.getBatchingMaxPublishDelayMs(), 30);
        assertEquals(spec.getRoundRobinRouterBatchingPartitionSwitchFrequency(), 10);
        assertEquals(spec.getBatchingMaxMessages(), 1000);
        assertEquals(spec.getBatchingMaxBytes(), 0);
        assertEquals(spec.getBatchBuilder(), "DEFAULT");
    }

    @Test
    public void testConvertFromSpec() {
        Function.BatchingSpec spec = Function.BatchingSpec.newBuilder()
                .setEnabled(true)
                .setBatchingMaxPublishDelayMs(30)
                .setRoundRobinRouterBatchingPartitionSwitchFrequency(10)
                .setBatchingMaxMessages(1000)
                .setBatchBuilder("DEFAULT")
                .build();
        BatchingConfig config = BatchingUtils.convertFromSpec(spec);
        assertEquals(config.isEnabled(), true);
        assertEquals(config.getBatchingMaxPublishDelayMs().intValue(), 30);
        assertEquals(config.getRoundRobinRouterBatchingPartitionSwitchFrequency().intValue(), 10);
        assertEquals(config.getBatchingMaxMessages().intValue(), 1000);
        assertEquals(config.getBatchingMaxBytes(), null);
        assertEquals(config.getBatchBuilder(), "DEFAULT");
    }

    @Test
    public void testConvertFromSpecFromNull() {
        BatchingConfig config = BatchingUtils.convertFromSpec(null);
        assertTrue(config.isEnabled());
        assertEquals(config.getBatchingMaxPublishDelayMs().intValue(), 10);
    }
}