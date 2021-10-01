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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

class PulsarLayoutManager implements LayoutManager {

    @Getter(AccessLevel.PACKAGE)
    private final MetadataStoreExtended store;

    @Getter(AccessLevel.PACKAGE)
    private final String ledgersRootPath;

    private final String layoutPath;

    PulsarLayoutManager(MetadataStoreExtended store, String ledgersRootPath) {
        this.ledgersRootPath = ledgersRootPath;
        this.store = store;
        this.layoutPath = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
    }

    @Override
    public LedgerLayout readLedgerLayout() throws IOException {
        try {
            byte[] layoutData = store.get(layoutPath).get()
                    .orElseThrow(() -> new BookieException.MetadataStoreException("Layout node not found"))
                    .getValue();
            return LedgerLayout.parseLayout(layoutData);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (BookieException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void storeLedgerLayout(LedgerLayout ledgerLayout) throws IOException {
        try {
            byte[] layoutData = ledgerLayout.serialize();

            store.put(layoutPath, layoutData, Optional.of(-1L)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MetadataStoreException.BadVersionException) {
                throw new LedgerLayoutExistsException(e);
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void deleteLedgerLayout() throws IOException {
        try {
            store.delete(layoutPath, Optional.empty()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }
}
