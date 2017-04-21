/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;

public class ManagedLedgerClientFactory implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerClientFactory.class);

    private final ManagedLedgerFactory managedLedgerFactory;

    private final List<ZooKeeper> zkClients;
    private final List<BookKeeper> bkClients;

    public ManagedLedgerClientFactory(PulsarService pulsar, ZooKeeperClientFactory zkClientFactory,
            BookKeeperClientFactory bookkeeperProvider) throws Exception {

        ServiceConfiguration conf = pulsar.getConfiguration();

        // Create multiple ZK session
        int numberOfZooKeeperSessions = conf.getManagedLedgerZooKeeperSessions();
        List<CompletableFuture<ZooKeeper>> futures = Lists.newArrayListWithCapacity(numberOfZooKeeperSessions);
        for (int i = 0; i < numberOfZooKeeperSessions; i++) {
            futures.add(zkClientFactory.create(conf.getZookeeperServers(), SessionType.ReadWrite,
                    (int) conf.getZooKeeperSessionTimeoutMillis()));
        }

        FutureUtil.waitForAll(futures).get();
        log.info("Successfully created {} ZooKeeper sesssion(s)", numberOfZooKeeperSessions);

        zkClients = new ArrayList<>(numberOfZooKeeperSessions);
        futures.forEach(f -> zkClients.add(f.getNow(null)));

        bkClients = new ArrayList<>(numberOfZooKeeperSessions);
        for (int i = 0; i < numberOfZooKeeperSessions; i++) {
            bkClients.add(bookkeeperProvider.create(conf, zkClients.get(i), pulsar.getIOEventLoopGroup()));
        }

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(conf.getManagedLedgerCacheSizeMB() * 1024L * 1024L);
        managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.getManagedLedgerCacheEvictionWatermark());

        this.managedLedgerFactory = new ManagedLedgerFactoryImpl(bkClients, zkClients, managedLedgerFactoryConfig);
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public void close() throws IOException {
        try {
            managedLedgerFactory.shutdown();
            log.info("Closed managed ledger factory");

            for (ZooKeeper zk : zkClients) {
                zk.close();
            }

            for (BookKeeper bk : bkClients) {
                bk.close();
            }
            log.info("Closed BookKeeper client");
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
