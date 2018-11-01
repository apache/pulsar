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
package org.apache.pulsar.sql.presto;

import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.shade.org.apache.bookkeeper.stats.StatsProvider;

public class PulsarConnectorCache {

    private static PulsarConnectorCache instance;

    private final ManagedLedgerFactory managedLedgerFactory;

    private final StatsProvider statsProvider;

    private PulsarConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        this.managedLedgerFactory = initManagedLedgerFactory(pulsarConnectorConfig);
        this.statsProvider = PulsarConnectorUtils.createInstance(pulsarConnectorConfig.getStatsProvider(),
                StatsProvider.class, getClass().getClassLoader());

        // start stats provider
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        pulsarConnectorConfig.getStatsProviderConfigs().forEach((key, value) -> clientConfiguration.setProperty(key, value));

        this.statsProvider.start(clientConfiguration);
    }

    public static PulsarConnectorCache getConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        synchronized (PulsarConnectorCache.class) {
            if (instance == null) {
                instance = new PulsarConnectorCache(pulsarConnectorConfig);
            }
        }
        return instance;
    }

    private static ManagedLedgerFactory initManagedLedgerFactory(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(pulsarConnectorConfig.getZookeeperUri())
                .setAllowShadedLedgerManagerFactoryClass(true)
                .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.")
                .setReadEntryTimeout(60);
        return new ManagedLedgerFactoryImpl(bkClientConfiguration);
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    public static void shutdown() throws ManagedLedgerException, InterruptedException {
        if (instance != null) {
            instance.managedLedgerFactory.shutdown();
            instance.statsProvider.stop();
            instance = null;
        }
    }
}
