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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class AtomicIntegerFlagsTest {

    @Test
    public void testEnablingAndDisablingFlags() {
        AtomicIntegerFlags flags = new AtomicIntegerFlags();
        // should return true when value is changed
        assertTrue(flags.changeFlag(5, true));
        assertTrue(flags.changeFlag(12, true));
        // should not return true when value isn't changed
        assertFalse(flags.changeFlag(5, true));
        for (int i = 0; i < 31; i++) {
            if (i == 5 || i == 12) {
                assertTrue(flags.getFlag(i));
            } else {
                assertFalse(flags.getFlag(i));
            }
        }
        assertTrue(flags.changeFlag(5, false));
        for (int i = 0; i < 31; i++) {
            if (i == 12) {
                assertTrue(flags.getFlag(i));
            } else {
                assertFalse(flags.getFlag(i));
            }
        }
    }

}