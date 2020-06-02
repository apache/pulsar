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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper utils.
 */
public final class ZkUtils {

    private static final Logger log = LoggerFactory.getLogger(ZkUtils.class);

    /**
     * Check if the provided <i>path</i> exists or not and wait it expired if possible.
     *
     * @param zk the zookeeper client instance
     * @param path the zookeeper path
     * @param sessionTimeoutMs session timeout in milliseconds
     * @return true if path exists, otherwise return false
     * @throws KeeperException when failed to access zookeeper
     * @throws InterruptedException interrupted when waiting for znode to be expired
     */
    public static boolean checkNodeAndWaitExpired(ZooKeeper zk,
                                                  String path,
                                                  long sessionTimeoutMs) throws KeeperException, InterruptedException {
        final CountDownLatch prevNodeLatch = new CountDownLatch(1);
        Watcher zkPrevNodeWatcher = watchedEvent -> {
            // check for prev node deletion.
            if (EventType.NodeDeleted == watchedEvent.getType()) {
                prevNodeLatch.countDown();
            }
        };
        Stat stat = zk.exists(path, zkPrevNodeWatcher);
        if (null != stat) {
            // if the ephemeral owner isn't current zookeeper client
            // wait for it to be expired
            if (stat.getEphemeralOwner() != zk.getSessionId()) {
                log.info("Previous znode : {} still exists, so waiting {} ms for znode deletion",
                    path, sessionTimeoutMs);
                if (!prevNodeLatch.await(sessionTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new NodeExistsException(path);
                } else {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
