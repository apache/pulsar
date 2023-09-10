/*
 *
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
 *
 */
package org.apache.bookkeeper.replication;

import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Utility class for replication tests.
 */
public class ReplicationTestUtil {

    /**
     * Checks whether ledger is in under-replication.
     */
    public static boolean isLedgerInUnderReplication(ZooKeeper zkc, long id,
            String basePath) throws KeeperException, InterruptedException {
        List<String> children;
        try {
            children = zkc.getChildren(basePath, true);
        } catch (KeeperException.NoNodeException nne) {
            return false;
        }

        boolean isMatched = false;
        for (String child : children) {
            if (child.startsWith("urL") && child.contains(String.valueOf(id))) {
                isMatched = true;
                break;
            } else {
                String path = basePath + '/' + child;
                try {
                    if (zkc.getChildren(path, false).size() > 0) {
                        isMatched = isLedgerInUnderReplication(zkc, id, path);
                    }
                } catch (KeeperException.NoNodeException nne) {
                    return false;
                }
            }

        }
        return isMatched;
    }
}
