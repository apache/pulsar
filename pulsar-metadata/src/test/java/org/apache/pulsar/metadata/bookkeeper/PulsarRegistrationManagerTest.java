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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link RegistrationManager}.
 */
@Slf4j
public class PulsarRegistrationManagerTest extends BaseMetadataStoreTest {

    private MetadataStoreExtended store;
    private RegistrationManager registrationManager;

    private String ledgersRootPath;


    private void methodSetup(Supplier<String> urlSupplier) throws Exception {
        this.ledgersRootPath = "/ledgers-" + UUID.randomUUID();
        this.store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        this.registrationManager = new PulsarRegistrationManager(store, ledgersRootPath, new ServerConfiguration());
    }

    @AfterMethod(alwaysRun = true)
    public final void methodCleanup() throws Exception {
        if (registrationManager != null) {
            registrationManager.close();
            registrationManager = null;
        }

        if (store != null) {
            store.close();
            store = null;
        }
    }

    @Test(dataProvider = "impl")
    public void testPrepareFormat(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        registrationManager.prepareFormat();
        assertTrue(store.exists(ledgersRootPath).join());
    }

    @Test(dataProvider = "impl")
    public void testGetClusterInstanceIdIfClusterNotInitialized(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        try {
            registrationManager.getClusterInstanceId();
            fail("Should fail getting cluster instance id if cluster not initialized");
        } catch (BookieException.MetadataStoreException e) {
            assertTrue(e.getMessage().contains("BookKeeper cluster not initialized"));
        }
    }

    @Test(dataProvider = "impl")
    public void testGetClusterInstanceId(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertClusterNotExists();
        registrationManager.initNewCluster();
        String instanceId = registrationManager.getClusterInstanceId();
        UUID uuid = UUID.fromString(instanceId);
        log.info("Cluster instance id : {}", uuid);
    }

    @Test(dataProvider = "impl")
    public void testNukeNonExistingCluster(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertClusterNotExists();
        assertTrue(registrationManager.nukeExistingCluster());
        assertClusterNotExists();
    }

    @Test(dataProvider = "impl")
    public void testNukeExistingCluster(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        assertTrue(registrationManager.nukeExistingCluster());
        assertClusterNotExists();
    }

    @Test(dataProvider = "impl")
    public void testInitNewClusterTwice(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        String instanceId = registrationManager.getClusterInstanceId();
        assertFalse(registrationManager.initNewCluster());
        assertClusterExists();
        assertEquals(instanceId, registrationManager.getClusterInstanceId());
    }

    @Test(dataProvider = "impl")
    public void testPrepareFormatNonExistingCluster(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertFalse(registrationManager.prepareFormat());
    }

    @Test(dataProvider = "impl")
    public void testPrepareFormatExistingCluster(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        assertTrue(registrationManager.prepareFormat());
    }

    @Test(dataProvider = "impl")
    public void testNukeExistingClusterWithWritableBookies(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        testNukeExistingClusterWithBookies(false);
    }

    @Test(dataProvider = "impl")
    public void testNukeExistingClusterWithReadonlyBookies(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        testNukeExistingClusterWithBookies(true);
    }

    private void testNukeExistingClusterWithBookies(boolean readonly) throws Exception {
        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        createNumBookies(3, readonly);
        assertFalse(registrationManager.nukeExistingCluster());
        assertClusterExists();
        removeNumBookies(3, readonly);
        assertTrue(registrationManager.nukeExistingCluster());
        assertClusterNotExists();
    }


    @Test(dataProvider = "impl")
    public void testNukeExistingClusterWithAllBookies(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        createNumBookies(1, false);
        createNumBookies(2, true);
        assertFalse(registrationManager.nukeExistingCluster());
        assertClusterExists();
        removeNumBookies(1, false);
        removeNumBookies(2, true);
        assertTrue(registrationManager.nukeExistingCluster());
        assertClusterNotExists();
    }

    @Test(dataProvider = "impl")
    public void testFormatNonExistingCluster(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertClusterNotExists();
        assertTrue(registrationManager.format());
        assertClusterExists();
    }

    @Test(dataProvider = "impl")
    public void testFormatExistingCluster(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertClusterNotExists();
        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        String clusterInstanceId = registrationManager.getClusterInstanceId();
        assertTrue(registrationManager.format());
        assertClusterExists();
        assertNotEquals(clusterInstanceId, registrationManager.getClusterInstanceId());
    }

    @Test(dataProvider = "impl")
    public void testFormatExistingClusterWithBookies(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);
        assertClusterNotExists();
        assertTrue(registrationManager.initNewCluster());
        assertClusterExists();
        String clusterInstanceId = registrationManager.getClusterInstanceId();
        createNumBookies(3, false);
        assertTrue(registrationManager.format());
        assertClusterExists();
    }

    private void createNumBookies(int numBookies, boolean readonly) throws Exception {
        for (int i = 0; i < numBookies; i++) {
            BookieId bookieId = BookieId.parse("bookie-" + i + ":3181");
            registrationManager.registerBookie(bookieId, readonly, new BookieServiceInfo());
        }
    }

    private void removeNumBookies(int numBookies, boolean readonly) throws Exception {
        for (int i = 0; i < numBookies; i++) {
            BookieId bookieId = BookieId.parse("bookie-" + i + ":3181");
            registrationManager.unregisterBookie(bookieId, readonly);
        }
    }

    private void assertClusterExists() {
        assertTrue(store.exists(ledgersRootPath + "/INSTANCEID").join());
    }

    private void assertClusterNotExists() {
        assertFalse(store.exists(ledgersRootPath + "/INSTANCEID").join());
    }
}
