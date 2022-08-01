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
package org.apache.pulsar.utils;

import org.apache.pulsar.broker.transaction.util.LogIndexLagBackoff;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class LogIndexLagBackOffTest {
    @Test
    public void testGenerateNextLogIndexLag() {
        LogIndexLagBackoff logIndexLagBackoff = new LogIndexLagBackoff(1, 10, 1);
        Assert.assertEquals(logIndexLagBackoff.next(0), 1);
        Assert.assertEquals(logIndexLagBackoff.next(6), 6);

        Assert.assertEquals(logIndexLagBackoff.next(77), 10);

        logIndexLagBackoff = new LogIndexLagBackoff(1, 10, 2);
        Assert.assertEquals(logIndexLagBackoff.next(3), 9);

        try {
            new LogIndexLagBackoff(-1, 2, 3);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "min lag must be > 0");
        }
        try {
            new LogIndexLagBackoff(2, 1, 3);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "maxLag should be >= minLag");
        }
        try {
            new LogIndexLagBackoff(1, 1, 0.2);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "exponent must be > 0");
        }

    }
}
