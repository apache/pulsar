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
package org.apache.pulsar.broker;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

public class ServiceConfigurationDefaultsTest {
    @Test
    public void defaultNumIOThreads() {
        // up to 4 processors, the previous default of twice the processors
        assertEquals(ServiceConfiguration.defaultNumIOThreads(1), 2);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(2), 4);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(3), 6);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(4), 8);
        // 8 from 4 up to 17 processors
        assertEquals(ServiceConfiguration.defaultNumIOThreads(5), 8);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(7), 8);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(8), 8);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(16), 8);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(17), 8);
        // half the processors from 18 up
        assertEquals(ServiceConfiguration.defaultNumIOThreads(18), 9);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(32), 16);
        assertEquals(ServiceConfiguration.defaultNumIOThreads(128), 64);
    }

    @Test
    public void numIOThreadsDefaultsToDefaultNumIOThreadsOfTheAvailableProcessors() {
        assertEquals(new ServiceConfiguration().getNumIOThreads(),
                ServiceConfiguration.defaultNumIOThreads(Runtime.getRuntime().availableProcessors()));
    }
}
