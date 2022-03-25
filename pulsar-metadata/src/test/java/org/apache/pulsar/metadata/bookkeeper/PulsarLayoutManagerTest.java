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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LayoutManager.LedgerLayoutExistsException;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class PulsarLayoutManagerTest extends BaseMetadataStoreTest {

    private static final int managerVersion = 0xabcd;

    private MetadataStoreExtended store;
    private LayoutManager layoutManager;

    private String ledgersRootPath;


    private void methodSetup(Supplier<String> urlSupplier) throws Exception {
        this.ledgersRootPath = "/ledgers-" + UUID.randomUUID();
        this.store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        this.layoutManager = new PulsarLayoutManager(store, ledgersRootPath);
    }

    @AfterMethod(alwaysRun = true)
    public final void methodCleanup() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    @Test(dataProvider = "impl")
    public void testReadCreateDeleteLayout(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        // layout doesn't exist
        try {
            layoutManager.readLedgerLayout();
            fail("should have failed");
        } catch (IOException e) {
            // Expected
        }

        // create the layout
        LedgerLayout layout = new LedgerLayout(
            PulsarLedgerManagerFactory.class.getName(),
            managerVersion
        );
        layoutManager.storeLedgerLayout(layout);

        // read the layout
        LedgerLayout readLayout = layoutManager.readLedgerLayout();
        assertEquals(layout, readLayout);

        // attempts to create the layout again and it should fail
        LedgerLayout newLayout = new LedgerLayout(
            "new layout",
            managerVersion + 1
        );
        try {
            layoutManager.storeLedgerLayout(newLayout);
            fail("Should fail storeLedgerLayout if layout already exists");
        } catch (LedgerLayoutExistsException e) {
            // expected
        }

        // read the layout again (layout should not be changed)
        readLayout = layoutManager.readLedgerLayout();
        assertEquals(layout, readLayout);

        // delete the layout
        layoutManager.deleteLedgerLayout();

        // the layout should be gone now
        try {
            layoutManager.readLedgerLayout();
            fail("should have failed");
        } catch (IOException e) {
            // Expected
        }

        // delete the layout again. it should fail since layout doesn't exist
        try {
            layoutManager.deleteLedgerLayout();
            fail("Should fail deleteLedgerLayout is layout not found");
        } catch (IOException ioe) {
            assertTrue(ioe.getMessage().contains("NotFoundException"));
        }
    }

}
