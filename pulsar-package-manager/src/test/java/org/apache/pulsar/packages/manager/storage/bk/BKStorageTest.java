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
 *
 */

package org.apache.pulsar.packages.manager.storage.bk;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BKStorageTest extends BookKeeperClusterTestCase {

    private PackageStorage packageStorage;
    private Namespace namespace;

    public BKStorageTest() {
        super(1);
    }

    @BeforeMethod
    public void setup() throws IOException {
        this.namespace = init();
        packageStorage = new BKPackageStorage(namespace);
    }

    private Namespace init() throws IOException {
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setWriteLockEnabled(false)
            .setOutputBufferSize(0)                  // 256k
            .setPeriodicFlushFrequencyMilliSeconds(0)         // disable periodical flush
            .setImmediateFlushEnabled(true)                  // disable immediate flush
            .setLogSegmentRollingIntervalMinutes(0)           // disable time-based rolling
            .setMaxLogSegmentBytes(Long.MAX_VALUE)            // disable size-based rolling
            .setExplicitTruncationByApplication(true)         // no auto-truncation
            .setRetentionPeriodHours(Integer.MAX_VALUE)       // long retention
            .setEnsembleSize(1)                     // replica settings
            .setWriteQuorumSize(1)
            .setAckQuorumSize(1)
            .setUseDaemonThread(true);

        BKDLConfig bkdlConfig = new BKDLConfig(zkUtil.getZooKeeperConnectString(), "/ledgers");
        DLMetadata dlMetadata = DLMetadata.create(bkdlConfig);
        URI dlogURI = URI.create(String.format("distributedlog://%s/pulsar/packages", zkUtil.getZooKeeperConnectString()));
        try {
            dlMetadata.create(dlogURI);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() != KeeperException.Code.NODEEXISTS) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Namespace n = NamespaceBuilder.newBuilder()
            .conf(conf)
            .clientId("test")
            .uri(dlogURI)
            .build();
        return n;
    }

    @AfterMethod
    public void teardown() throws ExecutionException, InterruptedException {
        if (packageStorage != null) {
            packageStorage.closeAsync().get();
        }
    }

    @Test
    public void testReadWriteOperations() throws ExecutionException, InterruptedException, IOException {
        String testData = "test-data";
        ByteArrayInputStream testDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
        String testPath = "test-read-write";

        // write some data to the dlog
        packageStorage.writeAsync(testPath, testDataStream).get();

        // read the data from the dlog
        ByteArrayOutputStream readData = new ByteArrayOutputStream();
        packageStorage.readAsync(testPath, readData).get();
        String readResult = new String(readData.toByteArray(), StandardCharsets.UTF_8);

        assertTrue(readResult.equals(testData));
    }

    @Test
    public void testReadNonExistentData() {
        String testPath = "non-existent-path";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            packageStorage.readAsync(testPath, outputStream).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof LogNotFoundException);
        }
    }

    @Test
    public void testListOperation() throws ExecutionException, InterruptedException {
        // write the data to different path
        String rootPath = "pulsar";
        String testData = "test-data";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));

        List<String> writePaths = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String path = "test-" + i;
            writePaths.add(path);
            packageStorage.writeAsync(rootPath + "/" + path, inputStream).get();
        }

        // list all path under the root path
        List<String> paths = packageStorage.listAsync(rootPath).get();

        // verify the paths number
        assertEquals(writePaths.size(), paths.size());
        paths.forEach(p -> writePaths.remove(p));
        assertEquals(0, writePaths.size());

        // list non-existent path
        try {
            packageStorage.listAsync("non-existent").get();
        } catch (Exception e) {
            // should not throw any exception
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeleteOperation() throws ExecutionException, InterruptedException {
        String testPath = "test-delete-path";
        String testData = "test-data";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));

        // write the data to the test path
        packageStorage.writeAsync(testPath, inputStream).get();

        // list path should have one file
        List<String> paths = packageStorage.listAsync("").get();
        assertEquals(1, paths.size());
        assertEquals(testPath, paths.get(0));

        // delete the path
        packageStorage.deleteAsync(testPath).get();

        // list again and not file under the path
        paths= packageStorage.listAsync("").get();
        assertEquals(0, paths.size());


        // delete non-existent path
        try {
            packageStorage.deleteAsync("non-existent").get();
            fail("should throw exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ZKException);
        }
    }

    @Test
    public void testExistOperation() throws ExecutionException, InterruptedException {
        Boolean exist = packageStorage.existAsync("test-path").get();
        assertFalse(exist);

        packageStorage.writeAsync("test-path", new ByteArrayInputStream("test".getBytes())).get();

        exist = packageStorage.existAsync("test-path").get();
        assertTrue(exist);
    }
}
