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
package org.apache.pulsar.common.allocator;

import static org.testng.Assert.assertEquals;
import java.util.Properties;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.testng.annotations.Test;

public class PulsarByteBufAllocatorTest {

    @Test
    public void testResolveLeakDetectionPolicyWithHighestLevel() {
        Properties properties = new Properties();
        properties.setProperty("io.netty.leakDetectionLevel", "paranoid");
        properties.setProperty("io.netty.leakDetection.level", "advanced");
        properties.setProperty("pulsar.allocator.leak_detection", "simple");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Paranoid);

        properties.setProperty("io.netty.leakDetectionLevel", "advanced");
        properties.setProperty("io.netty.leakDetection.level", "simple");
        properties.setProperty("pulsar.allocator.leak_detection", "paranoid");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Paranoid);

        properties.setProperty("io.netty.leakDetectionLevel", "simple");
        properties.setProperty("io.netty.leakDetection.level", "paranoid");
        properties.setProperty("pulsar.allocator.leak_detection", "advanced");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Paranoid);

        properties.setProperty("io.netty.leakDetectionLevel", "disabled");
        properties.setProperty("io.netty.leakDetection.level", "simple");
        properties.setProperty("pulsar.allocator.leak_detection", "disabled");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Simple);

        properties.setProperty("io.netty.leakDetectionLevel", "invalid");
        properties.setProperty("io.netty.leakDetection.level", "invalid");
        properties.setProperty("pulsar.allocator.leak_detection", "invalid");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Disabled);

        properties.clear();
        properties.setProperty("pulsar.allocator.leak_detection", "Paranoid");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Paranoid);

        properties.clear();
        properties.setProperty("io.netty.leakDetectionLevel", "Advanced");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Advanced);

        properties.clear();
        properties.setProperty("io.netty.leakDetection.level", "Simple");
        assertEquals(PulsarByteBufAllocator.resolveLeakDetectionPolicyWithHighestLevel(properties::getProperty),
                LeakDetectionPolicy.Simple);
    }
}