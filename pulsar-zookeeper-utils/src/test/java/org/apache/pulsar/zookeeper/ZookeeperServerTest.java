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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServerTest implements Closeable {
    private final File zkTmpDir;
    private ZooKeeperServer zks;
    private NIOServerCnxnFactory serverFactory;
    private final int zkPort;
    private final String hostPort;

    public ZookeeperServerTest(int zkPort) throws IOException {
        this.zkPort = zkPort;
        this.hostPort = "127.0.0.1:" + zkPort;
        this.zkTmpDir = File.createTempFile("zookeeper", "test");
        log.info("**** Start GZK on {} ****", zkTmpDir);
        if (!zkTmpDir.delete() || !zkTmpDir.mkdir()) {
            throw new IOException("Couldn't create zk directory " + zkTmpDir);
        }
    }

    public void start() throws IOException {
        try {
            zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
            zks.setMaxSessionTimeout(20000);
            serverFactory = new NIOServerCnxnFactory();
            serverFactory.configure(new InetSocketAddress(zkPort), 1000);
            serverFactory.startup(zks);
        } catch (Exception e) {
            log.error("Exception while instantiating ZooKeeper", e);
        }

        LocalBookkeeperEnsemble.waitForServerUp(hostPort, 30000);
        log.info("ZooKeeper started at {}", hostPort);
    }

    public void stop() throws IOException {
        zks.shutdown();
        serverFactory.shutdown();
        log.info("Stoppend ZK server at {}", hostPort);
    }

    @Override
    public void close() throws IOException {
        zks.shutdown();
        serverFactory.shutdown();
        zkTmpDir.delete();
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperServerTest.class);
}
