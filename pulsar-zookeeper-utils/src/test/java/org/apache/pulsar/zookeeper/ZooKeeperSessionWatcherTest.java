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
package org.apache.pulsar.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZooKeeperSessionWatcherTest {

    private class MockShutdownService implements ZooKeeperSessionWatcher.ShutdownService {
        private int exitCode = 0;

        @Override
        public void shutdown(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }

    private MockZooKeeper zkClient;
    private MockShutdownService shutdownService;
    private ZooKeeperSessionWatcher sessionWatcher;

    @BeforeMethod
    void setup() {
        zkClient = MockZooKeeper.newInstance();
        shutdownService = new MockShutdownService();
        sessionWatcher = new ZooKeeperSessionWatcher(zkClient, 1000, new ZookeeperSessionExpiredHandler() {

            private ZooKeeperSessionWatcher watcher;
            @Override
            public void onSessionExpired() {
                watcher.close();
                shutdownService.shutdown(-1);
            }

            @Override
            public void setWatcher(ZooKeeperSessionWatcher watcher) {
                this.watcher = watcher;
            }
        });
    }

    @AfterMethod
    void teardown() throws Exception {
        sessionWatcher.close();
        zkClient.shutdown();
    }

    @Test
    public void testProcess1() {
        WatchedEvent event = new WatchedEvent(EventType.None, KeeperState.Expired, null);
        sessionWatcher.process(event);
        assertTrue(sessionWatcher.isShutdownStarted());
        assertEquals(shutdownService.getExitCode(), -1);
    }

    @Test
    public void testProcess2() {
        WatchedEvent event = new WatchedEvent(EventType.None, KeeperState.Disconnected, null);
        sessionWatcher.process(event);
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
    public void testProcess3() {
        WatchedEvent event = new WatchedEvent(EventType.NodeCreated, KeeperState.Expired, null);
        sessionWatcher.process(event);
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
    public void testProcessResultConnectionLoss() {
        sessionWatcher.processResult(Code.CONNECTIONLOSS.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.Disconnected);
    }

    @Test
    public void testProcessResultSessionExpired() {
        sessionWatcher.processResult(Code.SESSIONEXPIRED.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.Expired);
    }

    @Test
    public void testProcessResultOk() {
        sessionWatcher.processResult(Code.OK.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
    }

    @Test
    public void testProcessResultNoNode() {
        sessionWatcher.processResult(Code.NONODE.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
    }

    @Test
    void testRun1() throws Exception {
        ZooKeeperSessionWatcher sessionWatcherZkNull = new ZooKeeperSessionWatcher(null, 1000,
            new ZookeeperSessionExpiredHandler() {
                private ZooKeeperSessionWatcher watcher;
                @Override
                public void onSessionExpired() {
                    watcher.close();
                    shutdownService.shutdown(-1);
                }

                @Override
                public void setWatcher(ZooKeeperSessionWatcher watcher) {
                    this.watcher = watcher;
                }
            });
        sessionWatcherZkNull.run();
        assertFalse(sessionWatcherZkNull.isShutdownStarted());
        assertEquals(sessionWatcherZkNull.getKeeperState(), KeeperState.Disconnected);
        assertEquals(shutdownService.getExitCode(), 0);
        sessionWatcherZkNull.close();
    }

    @Test
    void testRun2() throws Exception {
        ZooKeeperSessionWatcher sessionWatcherZkNull = new ZooKeeperSessionWatcher(null, 0,
            new ZookeeperSessionExpiredHandler() {

                private ZooKeeperSessionWatcher watcher;
                @Override
                public void onSessionExpired() {
                    watcher.close();
                    shutdownService.shutdown(-1);
                }

                @Override
                public void setWatcher(ZooKeeperSessionWatcher watcher) {
                    this.watcher = watcher;
                }
            });
        sessionWatcherZkNull.run();
        assertTrue(sessionWatcherZkNull.isShutdownStarted());
        assertEquals(sessionWatcherZkNull.getKeeperState(), KeeperState.Disconnected);
        assertEquals(shutdownService.getExitCode(), -1);
        sessionWatcherZkNull.close();
    }

    @Test
    public void testRun3() throws Exception {
        zkClient.shutdown();
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.Disconnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
    public void testRun4() throws Exception {
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
    public void testRun5() throws Exception {
        zkClient.create("/", new byte[0], null, null);
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
    public void testRun6() throws Exception {
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

}
