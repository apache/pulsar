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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class PortManagerTest {

    @Test
    public void allocatesAFreePort() {
        int port = PortManager.nextLockedFreePort();
        try {
            assertTrue(port > 0);
            assertTrue(PortManager.checkPortIfLocked(port));
        } finally {
            PortManager.releaseLockedPort(port);
        }
    }

    @Test
    public void allocatesDistinctPorts() {
        int p1 = PortManager.nextLockedFreePort();
        int p2 = PortManager.nextLockedFreePort();
        try {
            assertNotEquals(p1, p2);
        } finally {
            PortManager.releaseLockedPort(p1);
            PortManager.releaseLockedPort(p2);
        }
    }

    @Test
    public void releasingMarksPortAsUnlocked() {
        int port = PortManager.nextLockedFreePort();
        assertTrue(PortManager.checkPortIfLocked(port));
        assertTrue(PortManager.releaseLockedPort(port));
        assertFalse(PortManager.checkPortIfLocked(port));
    }
}
