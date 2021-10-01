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
package org.apache.pulsar.metadata.bookkeeper;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import java.util.List;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class PulsarLedgerManagerFactory implements LedgerManagerFactory {

    private static final int CUR_VERSION = 1;

    private AbstractConfiguration conf;
    private MetadataStoreExtended store;
    private String ledgerRootPath;

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf, LayoutManager layoutManager,
                                           int factoryVersion) throws IOException {

        checkArgument(layoutManager instanceof PulsarLayoutManager);

        PulsarLayoutManager pulsarLayoutManager = (PulsarLayoutManager) layoutManager;

        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        this.conf = conf;
        this.store = pulsarLayoutManager.getStore();
        this.ledgerRootPath = pulsarLayoutManager.getLedgersRootPath();
        return this;
    }

    @Override
    public void close() throws IOException {
        // since metadata store instance is passed from outside
        // we don't need to close it here
    }

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }


    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        return new PulsarLedgerIdGenerator(store, ledgerRootPath);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new PulsarLedgerManager(store, ledgerRootPath);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws ReplicationException.CompatibilityException {
        return new PulsarLedgerUnderreplicationManager(conf, store, ledgerRootPath);
    }

    @Override
    public LedgerAuditorManager newLedgerAuditorManager() throws IOException, InterruptedException {
        return new PulsarLedgerAuditorManager(store, ledgerRootPath);
    }

    @Override
    public void format(AbstractConfiguration<?> abstractConfiguration, LayoutManager layoutManager)
            throws InterruptedException, IOException {
        // TODO: XXX
    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf,
                                                  LayoutManager layoutManager)
            throws InterruptedException, IOException {
        @Cleanup
        PulsarLedgerManager ledgerManager = new PulsarLedgerManager(store, ledgerRootPath);

        /*
         * before proceeding with nuking existing cluster, make sure there
         * are no unexpected nodes under ledgersRootPath
         */
        List<String> ledgersRootPathChildrenList = store.getChildren(ledgerRootPath).join();
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if ((!AbstractZkLedgerManager.isSpecialZnode(ledgersRootPathChildren))
                    && (!ledgerManager.isLedgerParentNode(ledgersRootPathChildren))) {
                log.error("Found unexpected node : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, ledgerRootPath);
                return false;
            }
        }

        // formatting ledgermanager deletes ledger znodes
        format(conf, layoutManager);

        // now delete all the special nodes recursively
        for (String ledgersRootPathChildren : store.getChildren(ledgerRootPath).join()) {
            if (AbstractZkLedgerManager.isSpecialZnode(ledgersRootPathChildren)) {
                store.deleteRecursive(ledgerRootPath + "/" + ledgersRootPathChildren).join();
            } else {
                log.error("Found unexpected node : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, ledgerRootPath);
                return false;
            }
        }

        // finally deleting the ledgers rootpath
        store.deleteRecursive(ledgerRootPath).join();

        log.info("Successfully nuked existing cluster");
        return true;
    }
}
