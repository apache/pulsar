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
package org.apache.pulsar.packages.management.storage.bookkeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.PackagesStorageProvider;
import org.apache.pulsar.packages.management.core.impl.DefaultPackagesStorageConfiguration;
import org.apache.pulsar.packages.management.storage.bookkeeper.bookkeeper.test.BookKeeperClusterTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BookKeeperPackagesStorageTest extends BookKeeperClusterTestCase {
    private PackagesStorage storage;

    public BookKeeperPackagesStorageTest() {
        super(2);
    }

    @BeforeMethod()
    public void setup() throws Exception {
        PackagesStorageProvider provider = PackagesStorageProvider
            .newProvider(BookKeeperPackagesStorageProvider.class.getName());
        DefaultPackagesStorageConfiguration configuration = new DefaultPackagesStorageConfiguration();
        configuration.setProperty("metadataStoreUrl", zkUtil.getZooKeeperConnectString());
        configuration.setProperty("packagesReplicas", "1");
        configuration.setProperty("packagesManagementLedgerRootPath", "/ledgers");
        storage = provider.getStorage(configuration);
        storage.initialize();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (storage != null) {
            storage.closeAsync().get();
        }
    }

    @Test(timeOut = 60000)
    public void testConfiguration() {
        assertTrue(storage instanceof BookKeeperPackagesStorage);
        BookKeeperPackagesStorage bkStorage = (BookKeeperPackagesStorage) storage;
        assertEquals(bkStorage.configuration.getMetadataStoreUrl(), zkUtil.getZooKeeperConnectString());
        assertEquals(bkStorage.configuration.getPackagesReplicas(), 1);
        assertEquals(bkStorage.configuration.getPackagesManagementLedgerRootPath(), "/ledgers");
    }

    @Test(timeOut = 60000)
    public void testReadWriteOperations() throws ExecutionException, InterruptedException {
        String testData = "test-data";
        ByteArrayInputStream testDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
        String testPath = "test-read-write";

        // write some data to the dlog
        storage.writeAsync(testPath, testDataStream).get();

        // read the data from the dlog
        ByteArrayOutputStream readData = new ByteArrayOutputStream();
        storage.readAsync(testPath, readData).get();
        String readResult = new String(readData.toByteArray(), StandardCharsets.UTF_8);

        assertEquals(testData, readResult);
    }

    @Test(timeOut = 60000)
    public void testReadWriteLargeDataOperations() throws ExecutionException, InterruptedException {
        byte[] data = RandomUtils.nextBytes(8192 * 3 + 4096);
        ByteArrayInputStream testDataStream = new ByteArrayInputStream(data);
        String testPath = "test-large-read-write";

        // write some data to the dlog
        storage.writeAsync(testPath, testDataStream).get();

        // read the data from the dlog
        ByteArrayOutputStream readData = new ByteArrayOutputStream();
        storage.readAsync(testPath, readData).get();
        byte[] readResult = readData.toByteArray();

        assertEquals(data, readResult);
    }

    @Test(timeOut = 60000)
    public void testReadNonExistentData() {
        String testPath = "non-existent-path";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            storage.readAsync(testPath, outputStream).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof LogNotFoundException);
        }
    }

    @Test(timeOut = 60000)
    public void testListOperation() throws ExecutionException, InterruptedException {
        // write the data to different path
        String rootPath = "pulsar";
        String testData = "test-data";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));

        List<String> writePaths = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String path = "test-" + i;
            writePaths.add(path);
            storage.writeAsync(rootPath + "/" + path, inputStream).get();
        }

        // list all path under the root path
        List<String> paths = storage.listAsync(rootPath).get();

        // verify the paths number
        assertEquals(paths.size(), writePaths.size());
        paths.forEach(p -> writePaths.remove(p));
        assertEquals(writePaths.size(), 0);

        // list non-existent path
        try {
            storage.listAsync("non-existent").get();
        } catch (Exception e) {
            // should not throw any exception
            fail(e.getMessage());
        }
    }

    @Test(timeOut = 60000)
    public void testDeleteOperation() throws ExecutionException, InterruptedException {
        String testPath = "test-delete-path";
        String testData = "test-data";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));

        // write the data to the test path
        storage.writeAsync(testPath, inputStream).get();

        // list path should have one file
        List<String> paths = storage.listAsync("").get();
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), testPath);

        // delete the path
        storage.deleteAsync(testPath).get();

        // list again and not file under the path
        paths= storage.listAsync("").get();
        assertEquals(paths.size(), 0);


        // delete non-existent path
        try {
            storage.deleteAsync("non-existent").get();
            fail("should throw exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ZKException);
        }
    }

    @Test(timeOut = 60000)
    public void testExistOperation() throws ExecutionException, InterruptedException {
        Boolean exist = storage.existAsync("test-path").get();
        org.testng.Assert.assertFalse(exist);

        storage.writeAsync("test-path", new ByteArrayInputStream("test".getBytes())).get();

        exist = storage.existAsync("test-path").get();
        assertTrue(exist);
    }

    @Test(timeOut = 60000)
    public void testReadWriteOperationsWithSeparatedBkCluster() throws Exception {
        PackagesStorageProvider provider = PackagesStorageProvider
                .newProvider(BookKeeperPackagesStorageProvider.class.getName());
        DefaultPackagesStorageConfiguration configuration = new DefaultPackagesStorageConfiguration();
        // set the unavailable bk cluster with mock zookeeper path
        configuration.setProperty("metadataStoreUrl", zkUtil.getZooKeeperConnectString() + "/mock");
        configuration.setProperty("packagesReplicas", "1");
        configuration.setProperty("packagesManagementLedgerRootPath", "/ledgers");
        PackagesStorage storage1 = provider.getStorage(configuration);
        storage1.initialize();

        String mockData = "mock-data";
        ByteArrayInputStream mockDataStream = new ByteArrayInputStream(mockData.getBytes(StandardCharsets.UTF_8));
        String mockPath = "mock-path";

        // write some data to the dlog will fail
        try {
            storage1.writeAsync(mockPath, mockDataStream).get();
        } catch (Exception e) {
            String errMsg = e.getCause().getMessage();
            assertTrue(errMsg.contains("Error on allocating ledger") || errMsg.contains("Write rejected"));
        } finally {
            storage1.closeAsync().get();
        }

        // set the available bk cluster with bookkeeperMetadataServiceUri using actual zookeeper path
        String bookkeeperMetadataServiceUri = String.format("zk+null://%s/ledgers", zkUtil.getZooKeeperConnectString());
        DefaultPackagesStorageConfiguration configuration2 = new DefaultPackagesStorageConfiguration();
        configuration2.setProperty("metadataStoreUrl", zkUtil.getZooKeeperConnectString());
        configuration2.setProperty("bookkeeperMetadataServiceUri", bookkeeperMetadataServiceUri);
        configuration2.setProperty("packagesReplicas", "1");
        PackagesStorage storage2 = provider.getStorage(configuration2);
        storage2.initialize();

        String testData = "test-data";
        ByteArrayInputStream testDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
        String testPath = "test-path";

        // write some data to the dlog will success
        try {
            storage2.writeAsync(testPath, testDataStream).get();

            // read the data from the dlog
            ByteArrayOutputStream readData = new ByteArrayOutputStream();
            storage2.readAsync(testPath, readData).get();
            String readResult = new String(readData.toByteArray(), StandardCharsets.UTF_8);

            assertEquals(testData, readResult);
        } finally {
            storage2.closeAsync().get();
        }
    }

}
