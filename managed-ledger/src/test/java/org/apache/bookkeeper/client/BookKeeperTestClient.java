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
package org.apache.bookkeeper.client;

import java.io.IOException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Test BookKeeperClient which allows access to members we don't wish to expose in the public API.
 */
public class BookKeeperTestClient extends BookKeeper {
    public BookKeeperTestClient(ClientConfiguration conf) throws IOException, InterruptedException, BKException {
        super(conf);
    }

    public ZooKeeper getZkHandle() {
        return super.getZkHandle();
    }

    public ClientConfiguration getConf() {
        return super.getConf();
    }

    /**
     * Force a read to zookeeper to get list of bookies.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void readBookiesBlocking() throws InterruptedException, BKException {
        bookieWatcher.initialBlockingBookieRead();
    }
}
