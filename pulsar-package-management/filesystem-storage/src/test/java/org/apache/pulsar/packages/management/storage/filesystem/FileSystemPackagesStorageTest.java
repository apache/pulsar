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
package org.apache.pulsar.packages.management.storage.filesystem;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.PackagesStorageProvider;
import org.apache.pulsar.packages.management.core.impl.DefaultPackagesStorageConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class FileSystemPackagesStorageTest {
    private PackagesStorage storage;
    private Path storagePath;

    @BeforeMethod()
    public void setup() throws Exception {
        this.storagePath = Files.createTempDirectory("package-storage-test");
        log.info("Test using storage path: {}", storagePath);

        PackagesStorageProvider provider = PackagesStorageProvider
            .newProvider(FileSystemPackagesStorageProvider.class.getName());
        DefaultPackagesStorageConfiguration configuration = new DefaultPackagesStorageConfiguration();
        configuration.setProperty("STORAGE_PATH", storagePath.toString());
        storage = provider.getStorage(configuration);
        storage.initialize();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (storage != null) {
            storage.closeAsync().get();
        }

        storagePath.toFile().delete();
    }

    @Test(timeOut = 60000)
    public void testReadWriteOperations() throws ExecutionException, InterruptedException {
        String testData = "test-data";
        ByteArrayInputStream testDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
        String testPath = "test-read-write";

        // write some data to the package
        storage.writeAsync(testPath, testDataStream).get();

        // read the data from the package
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

        // write some data to the package
        storage.writeAsync(testPath, testDataStream).get();

        // read the data from the package
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
            storage.readAsync(testPath, outputStream).join();
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), FileNotFoundException.class);
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
        storage.listAsync("non-existent").get();
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
            storage.deleteAsync("non-existent").join();
            fail("should throw exception");
        } catch (Exception e) {
            assertEquals(e.getCause().getClass(), IOException.class);
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

}
