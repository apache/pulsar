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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IsBelowLoadLevelTest {
    @DataProvider
    public Object[][] resourcesAndLevels() {
        return new Object[][] {
                {0, 49, true}, {0, 50, false}, {0, 51, false},
                {1, 49, true}, {1, 50, false}, {1, 51, false},
                {2, 49, true}, {2, 50, false}, {2, 51, false},
                {3, 49, true}, {3, 50, false}, {3, 51, false}
        };
    }

    @Test(dataProvider = "resourcesAndLevels")
    public void testEachResource(int resource, int level, boolean expected) {
        SystemResourceUsage usage = new SystemResourceUsage();
        usage.bandwidthOut = new ResourceUsage(resource == 0 ? level : 49, 100);
        usage.bandwidthIn = new ResourceUsage(resource == 1 ? level : 49, 100);
        usage.cpu = new ResourceUsage(resource == 2 ? level : 49, 100);
        usage.directMemory = new ResourceUsage(resource == 3 ? level : 49, 100);
        usage.memory = null;
        assertThat(SystemResourceUsage.isBelowLoadLevel(usage, 50)).isEqualTo(expected);
        assertThat(SimpleLoadManagerImpl.isBelowLoadLevel(usage, 50)).isEqualTo(expected);
    }

    @Test
    public void testNullAndShortCircuit() {
        assertThatNullPointerException().isThrownBy(() -> SimpleLoadManagerImpl.isBelowLoadLevel(null, 50));
        SystemResourceUsage usage = new SystemResourceUsage();
        usage.bandwidthOut = new ResourceUsage(50, 100);
        usage.bandwidthIn = null;
        assertThat(SimpleLoadManagerImpl.isBelowLoadLevel(usage, 50)).isFalse();
        usage.bandwidthOut = new ResourceUsage(49, 100);
        assertThatNullPointerException().isThrownBy(() -> SimpleLoadManagerImpl.isBelowLoadLevel(usage, 50));
    }
}
