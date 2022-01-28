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
package org.apache.pulsar.packages.management.core.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.packages.management.core.PackagesManagement;
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.PackagesStorageProvider;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.packages.management.core.common.PackageMetadataUtil;
import org.apache.pulsar.packages.management.core.common.PackageName;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PackagesManagementImplTest {
    private static PackagesStorage storage;
    private static PackagesManagement packagesManagement;

    @BeforeClass
    public static void setup() throws IOException {
        PackagesStorageProvider storageProvider = PackagesStorageProvider.newProvider(MockedPackagesStorageProvider.class.getName());
        DefaultPackagesStorageConfiguration packagesStorageConfiguration = new DefaultPackagesStorageConfiguration();
        storage = storageProvider.getStorage(packagesStorageConfiguration);

        packagesManagement = new PackagesManagementImpl();
        packagesManagement.initialize(storage);
    }

    @AfterClass(alwaysRun = true)
    public static void teardown() throws ExecutionException, InterruptedException {
        storage.closeAsync().get();
    }


    @Test
    public void testPackagesManagementFlow() {
        PackageName packageName = PackageName.get("function://tenant/ns/non-existent-package@v1");
        // get a non-existent package metadata should fail
        try {
            packagesManagement.getMeta(packageName).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // update a non-existent package metadata should fail
        PackageMetadata failedUpdateMetadata = PackageMetadata.builder()
            .description("Failed update package metadata").build();
        try {
            packagesManagement.updateMeta(packageName, failedUpdateMetadata).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // download a non-existent package should fail
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            packagesManagement.download(packageName, outputStream).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // delete a non-existent package should fail
        try {
            packagesManagement.delete(packageName).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // list a non-existent package version should fail
        try {
            packagesManagement.list(packageName).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // list the packages in a non-existent namespace should fail
        try {
            packagesManagement.list(packageName.getPkgType(), packageName.getTenant(), packageName.getNamespace()).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // upload a package
        PackageMetadata metadata = PackageMetadata.builder()
            .contact("test@apache.org")
            .description("A mocked test package")
            .createTime(System.currentTimeMillis()).build();
        try (ByteArrayInputStream inputStream= new ByteArrayInputStream(PackageMetadataUtil.toBytes(metadata))) {
            packagesManagement.upload(packageName, metadata, inputStream).get();
        } catch (Exception e) {
            Assert.fail("should not throw any exception");
        }

        // get an existent package metadata should succeed
        try {
            PackageMetadata getPackageMetadata = packagesManagement.getMeta(packageName).get();
            Assert.assertEquals(metadata, getPackageMetadata);
        } catch (Exception e) {
            Assert.fail("should not throw any exception");
        }

        // download an existent package should succeed
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            packagesManagement.download(packageName, outputStream).get();
            PackageMetadata getPackage = PackageMetadataUtil.fromBytes(outputStream.toByteArray());
            Assert.assertEquals(metadata, getPackage);
        } catch (Exception e) {
            Assert.fail("should not throw any exception");
        }

        // update an existent package metadata should succeed
        metadata.setModificationTime(System.currentTimeMillis());
        try {
            packagesManagement.updateMeta(packageName, metadata).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // get the updated metadata
        try {
            PackageMetadata updatedMetadata = packagesManagement.getMeta(packageName).get();
            Assert.assertEquals(metadata, updatedMetadata);
        } catch (Exception e) {
            Assert.fail("should not throw any exception");
        }

        // list an existent package version should success
        try {
            List<String> versions = packagesManagement.list(packageName).get();
            Assert.assertEquals(1, versions.size());
            Assert.assertEquals(packageName.getVersion(), versions.get(0));
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }

        // list the packages in a non-existent namespace should fail
        try {
            List<String> packageNames = packagesManagement
                .list(packageName.getPkgType(), packageName.getTenant(), packageName.getNamespace()).get();
            Assert.assertEquals(1, packageNames.size());
            Assert.assertEquals(packageName.getName(), packageNames.get(0));
        } catch (Exception e) {
            if (!(e.getCause() instanceof PackagesManagementException.NotFoundException)) {
                Assert.fail("should not throw any exception");
            }
        }


        // delete an existent package should succeed
        try {
            packagesManagement.delete(packageName).get();
        } catch (Exception e) {
            Assert.fail("should not throw any exception");
        }
    }
}
