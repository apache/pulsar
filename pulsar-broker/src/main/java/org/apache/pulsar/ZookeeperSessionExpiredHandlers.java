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
package org.apache.pulsar;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher;
import org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher.ShutdownService;
import org.apache.pulsar.zookeeper.ZookeeperSessionExpiredHandler;

/**
 * Handlers for broker service to handle Zookeeper session expired
 */
public class ZookeeperSessionExpiredHandlers {

    public static final String SHUTDOWN_POLICY = "shutdown";
    public static final String RECONNECT_POLICY = "reconnect";

    public static ZookeeperSessionExpiredHandler shutdownWhenZookeeperSessionExpired(ShutdownService shutdownService) {
        return new ShutDownWhenSessionExpired(shutdownService);
    }

    public static ZookeeperSessionExpiredHandler reconnectWhenZookeeperSessionExpired(PulsarService pulsarService, ShutdownService shutdownService) {
        return new ReconnectWhenSessionExpired(pulsarService, shutdownService);
    }

    // Shutdown the messaging service when Zookeeper session expired.
    public static class ShutDownWhenSessionExpired implements ZookeeperSessionExpiredHandler {

        private final ShutdownService shutdownService;
        private ZooKeeperSessionWatcher watcher;

        public ShutDownWhenSessionExpired(ShutdownService shutdownService) {
            this.shutdownService = shutdownService;
        }

        @Override
        public void setWatcher(ZooKeeperSessionWatcher watcher) {
            this.watcher = watcher;
        }

        @Override
        public void onSessionExpired() {
            this.watcher.close();
            this.shutdownService.shutdown(-1);
        }
    }

    // Reconnect to the zookeeper server and re-register ownership cache to avoid ownership change.
    public static class ReconnectWhenSessionExpired implements ZookeeperSessionExpiredHandler {

        private final PulsarService pulsarService;
        private ZooKeeperSessionWatcher watcher;
        private final ShutdownService shutdownService;

        public ReconnectWhenSessionExpired(PulsarService pulsarService, ShutdownService shutdownService) {
            this.pulsarService = pulsarService;
            this.shutdownService = shutdownService;
        }

        @Override
        public void onSessionExpired() {
            if (this.pulsarService.getNamespaceService() == null) {
                this.watcher.close();
                this.shutdownService.shutdown(-1);
            }
            this.pulsarService.getNamespaceService().registerOwnedBundles();
        }

        @Override
        public void setWatcher(ZooKeeperSessionWatcher watcher) {
            this.watcher = watcher;
        }
    }
}
