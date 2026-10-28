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
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;

/**
 * Runs {@link ManagedLedgerReadHandleMetadataTest} with the managed ledger reading through the metadata driver based
 * on the Pulsar metadata store, which notifies the ledger metadata updates differently than the ZooKeeper driver of
 * BookKeeper. The bookies and the re-replication keep the ZooKeeper driver, which uses the same layout.
 */
public class ManagedLedgerReadHandlePulsarMetadataDriverTest extends ManagedLedgerReadHandleMetadataTest {

    public ManagedLedgerReadHandlePulsarMetadataDriverTest() throws Exception {
        // The driver registers its scheme when it is loaded
        Class.forName(PulsarMetadataClientDriver.class.getName());
    }

    @Override
    protected String getReaderMetadataServiceUri() {
        // This driver uses the default ledgers root path, which is the one of this test
        return AbstractMetadataDriver.METADATA_STORE_SCHEME + ":" + zkUtil.getZooKeeperConnectString();
    }

    @Override
    protected Class<? extends MetadataClientDriver> getReaderMetadataDriverClass() {
        return PulsarMetadataClientDriver.class;
    }
}
