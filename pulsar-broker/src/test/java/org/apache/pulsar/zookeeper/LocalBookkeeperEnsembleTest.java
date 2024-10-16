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
package org.apache.pulsar.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LocalBookkeeperEnsembleTest {

    @BeforeMethod
    void setup() throws Exception {
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
    }

    @Test
    public void testStartStop() throws Exception {

        final int numBk = 1;

        // Start local Bookies/ZooKeepers and confirm that they are running at specified ports
        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(numBk, 0, () -> 0);
        ensemble.start();
        assertTrue(ensemble.getZkServer().isRunning());
        assertEquals(ensemble.getZkServer().getClientPort(), ensemble.getZookeeperPort());
        assertTrue(ensemble.getZkClient().getState().isConnected());
        assertTrue(ensemble.getBookies()[0].isRunning());

        // Stop local Bookies/ZooKeepers and confirm that they are correctly closed
        ensemble.stop();
        assertFalse(ensemble.getZkServer().isRunning());
        assertFalse(ensemble.getZkClient().getState().isConnected());
        assertFalse(ensemble.getBookies()[0].isRunning());
    }

    @Test(timeOut = 10_000)
    public void testStartWithSpecifiedStreamStoragePort() throws Exception {
        LocalBookkeeperEnsemble ensemble = null;
        try {
            ensemble =
                    new LocalBookkeeperEnsemble(1, 0, 0, 4182, null, null, true, null);
            ensemble.startStandalone(new ServerConfiguration(), true);
        } finally {
            if (ensemble != null) {
                ensemble.stop();
            }
        }
    }
}
