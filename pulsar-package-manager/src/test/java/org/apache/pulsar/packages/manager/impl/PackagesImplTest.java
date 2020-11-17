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

package org.apache.pulsar.packages.manager.impl;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.exception.PackageMetaAlreadyExistsException;
import org.apache.pulsar.packages.manager.exception.PackageMetaNotFoundException;
import org.apache.pulsar.packages.manager.exception.PackageNotFoundException;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;
import org.apache.pulsar.packages.manager.storage.bk.BKPackageStorage;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PackagesImplTest extends BookKeeperClusterTestCase {

    private PackageImpl pm;
    private PackageStorage packageStorage;

    public PackagesImplTest() {
        super(1);
    }

    @BeforeMethod
    public void setup() throws IOException, ExecutionException, InterruptedException {
        packageStorage = new BKPackageStorage(init());
        this.pm = new PackageImpl(packageStorage);
        uploadTestData();
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

        return NamespaceBuilder.newBuilder()
            .conf(conf)
            .clientId("test")
            .uri(dlogURI)
            .build();
    }

    @DataProvider(name = "packagesProvider")
    private Object[][] packagesDataProvider() {
        return new String[][]{
            // package contact, package description, package name, package data
            {"test-A", "test-A-function-1", "function://public/A/f@v1", "function-1"},
            {"test-A", "test-A-function-2", "function://public/A/f@v2", "function-2"},
            {"test-A", "test-A-function-3", "function://public/A/f-1@v1", "function-f1"},

            {"test-A", "test-A-source-1", "source://public/A/so@v1", "source-1"},
            {"test-A", "test-A-source-2", "source://public/A/so@v2", "source-2"},
            {"test-A", "test-A-source-3", "source://public/A/so-1@v1", "source-s1"},

            {"test-A", "test-A-sink-1", "sink://public/A/si@v1", "sink-1"},
            {"test-A", "test-A-sink-2", "sink://public/A/si@v2", "sink-2"},
            {"test-A", "test-A-sink-3", "sink://public/A/si-1@v1", "sink-s1"},
        };
    }

    private void uploadTestData() throws ExecutionException, InterruptedException {
        String[][] datas = (String[][]) packagesDataProvider();
        for (int i = 0; i < datas.length; i++) {
            String[] info = datas[i];
            PackageName name = PackageName.get(info[2]);
            PackageMetadata metadata = PackageMetadata.builder()
                .contact(info[0])
                .description(info[1])
                .createTime(System.currentTimeMillis())
                .build();
            byte[] pData = info[3].getBytes(StandardCharsets.UTF_8);
            pm.upload(name, metadata, new ByteArrayInputStream(pData)).get();
        }
    }

    @Test
    public void testMetaOperations() throws ExecutionException, InterruptedException {
        String contact = "test-A";
        String description = "test-A-function-1";
        String packageName = "function://public/A/f@v1";

        PackageMetadata metadata = pm.getMeta(PackageName.get(packageName)).get();
        assertEquals(contact, metadata.getContact());
        assertEquals(description, metadata.getDescription());
        assertTrue(metadata.getCreateTime() < System.currentTimeMillis());

        long updateTime = System.currentTimeMillis();
        PackageMetadata updateMeta = PackageMetadata.builder()
            .contact("update-" + contact)
            .description("update-" + description)
            .modificationTime(updateTime)
            .build();
        pm.updateMeta(PackageName.get(packageName), updateMeta).get();

        metadata = pm.getMeta(PackageName.get(packageName)).get();
        assertEquals(updateMeta.getContact(), metadata.getContact());
        assertEquals(updateMeta.getDescription(), metadata.getDescription());
        assertEquals(updateMeta.getModificationTime(), metadata.getModificationTime());
    }

    @Test
    public void testMetaOperationsFailed() {
        // get non-existent meta
        try {
            pm.getMeta(PackageName.get("function://public/default/non-existent")).get();
            fail("should throw metadata not found exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageMetaNotFoundException);
        }

        try {
            pm.updateMeta(PackageName.get("function://public/default/non-existent"), PackageMetadata.builder().build()).get();
            fail("should throw metadata not found exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageMetaNotFoundException);
        }
    }

    @Test
    public void testListOperation() throws ExecutionException, InterruptedException {
        // list all version of a package
        PackageName functionA = PackageName.get("function://public/A/f");
        PackageName sinkA = PackageName.get("sink://public/A/si");
        PackageName sourceA = PackageName.get("source://public/A/so");

        List<PackageName> functionList = pm.list(functionA).get();
        assertEquals(2, functionList.size());
        assertTrue(functionList.contains(PackageName.get("function://public/A/f@v1")));
        assertTrue(functionList.contains(PackageName.get("function://public/A/f@v2")));

        List<PackageName> sinkList = pm.list(sinkA).get();
        assertEquals(2, sinkList.size());
        assertTrue(sinkList.contains(PackageName.get("sink://public/A/si@v1")));
        assertTrue(sinkList.contains(PackageName.get("sink://public/A/si@v2")));

        List<PackageName> sourceList = pm.list(sourceA).get();
        assertEquals(2, sourceList.size());
        assertTrue(sourceList.contains(PackageName.get("source://public/A/so@v1")));
        assertTrue(sourceList.contains(PackageName.get("source://public/A/so@v2")));

        // list all specified type packages in a namespace
//        functionList = pm.list(functionA.getPkgType(), functionA.getNamespaceName()).get();
//        assertEquals(2, functionList.size());
//        assertTrue(functionList.contains(PackageName.get("function://public/A/f@latest")));
//        assertTrue(functionList.contains(PackageName.get("function://public/A/f-1@latest")));
//
//        sinkList = pm.list(sinkA.getPkgType(), sinkA.getNamespaceName()).get();
//        assertEquals(2, sinkList.size());
//        assertTrue(sinkList.contains(PackageName.get("sink://public/A/si@latest")));
//        assertTrue(sinkList.contains(PackageName.get("sink://public/A/si-1@latest")));
//
//        sourceList = pm.list(sourceA.getPkgType(), sourceA.getNamespaceName()).get();
//        assertEquals(2, sourceList.size());
//        assertTrue(sourceList.contains(PackageName.get("source://public/A/so@latest")));
//        assertTrue(sourceList.contains(PackageName.get("source://public/A/so-1@latest")));
    }

    @Test
    public void testListNonExistentPackage() {
        try {
            List<PackageName> p = pm.list(PackageName.get("function://public/default/non-existent")).get();
            assertEquals(0, p.size());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            List<PackageName> p = pm.list(PackageType.FUNCTION, NamespaceName.get("public/non-existent")).get();
            assertEquals(0, p.size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test(dataProvider = "packagesProvider")
    public void testDownloadAndDeleteOperation(String contact, String description, String packageName, String packageData) throws ExecutionException, InterruptedException {
        // download packages
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        pm.download(PackageName.get(packageName), outputStream).get();
        String data = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        assertEquals(packageData, data);

        // delete packages
        pm.delete(PackageName.get(packageName)).get();

        // download again, should thrown package not found exception
        ByteArrayOutputStream failedOutput = new ByteArrayOutputStream();
        try {
            pm.download(PackageName.get(packageName), failedOutput).get();
            fail("should throw package not found exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageNotFoundException);
        }
    }

    @Test
    public void testDeleteNonExistentPackage() {
        try {
            pm.delete(PackageName.get("function://public/default/non-existent")).get();
        } catch (Exception e) {
            // should not thrown any exception
            fail(e.getMessage());
        }
    }

    @Test(dataProvider = "packagesProvider")
    public void testUploadDuplicate(String contact, String description, String packageName, String packageData) {
        PackageMetadata metadata = PackageMetadata.builder()
            .contact(contact)
            .description(description)
            .createTime(System.currentTimeMillis())
            .build();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(packageData.getBytes(StandardCharsets.UTF_8));

        try {
            pm.upload(PackageName.get(packageName), metadata, inputStream).get();
            fail("should throw package meta already exist");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageMetaAlreadyExistsException);
        }
    }
}
