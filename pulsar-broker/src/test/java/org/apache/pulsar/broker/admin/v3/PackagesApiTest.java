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
package org.apache.pulsar.broker.admin.v3;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class PackagesApiTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setEnablePackagesManagement(true);
        conf.setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60000)
    public void testPackagesOperations() throws Exception {
        // create a temp file for testing

        File file = File.createTempFile("package-api-test", ".package");

        // testing upload api
        String packageName = "function://public/default/test@v1";
        PackageMetadata originalMetadata = PackageMetadata.builder()
            .language("golang").functionClassname("Test").description("test").build();
        admin.packages().upload(originalMetadata, packageName, file.getPath());

        // testing download api
        String directory = file.getParent();
        String downloadPath = directory + "package-api-test-download.package";
        admin.packages().download(packageName, downloadPath);
        File downloadFile = new File(downloadPath);
        assertTrue(downloadFile.exists());
        downloadFile.delete();

        // testing list packages api
        List<String> packages = admin.packages().listPackages("function", "public/default");
        assertEquals(packages.size(), 1);
        assertEquals(packages.get(0), "test");

        // testing list versions api
        List<String> versions = admin.packages().listPackageVersions(packageName);
        assertEquals(versions.size(), 1);
        assertEquals(versions.get(0), "v1");

        // testing get packages api
        PackageMetadata metadata = admin.packages().getMetadata(packageName);
        assertEquals(metadata.getDescription(), originalMetadata.getDescription());
        assertNull(metadata.getContact());
        assertTrue(metadata.getModificationTime() > 0);
        assertTrue(metadata.getCreateTime() > 0);
        assertNull(metadata.getProperties());

        // testing update package metadata api
        PackageMetadata updatedMetadata = PackageMetadata.builder()
            .contact("test@apache.org")
            .properties(Collections.singletonMap("key", "value")).build();
        admin.packages().updateMetadata(packageName, updatedMetadata);

        PackageMetadata getUpdatedMetadata = admin.packages().getMetadata(packageName);
        assertEquals(getUpdatedMetadata.getDescription(), updatedMetadata.getDescription());
        assertEquals(getUpdatedMetadata.getContact(), updatedMetadata.getContact());
        assertEquals(getUpdatedMetadata.getProperties(), updatedMetadata.getProperties());

        // update the package language or function classname
        PackageMetadata wrongMetadata = PackageMetadata.builder()
            .language("python").build();
        try {
            admin.packages().updateMetadata(packageName, wrongMetadata);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(400, e.getStatusCode());
        }
    }

    @Test(timeOut = 60000)
    public void testPackagesOperationsFailed() throws IOException{
        // Upload a package without specify the package language or classname
        File file = File.createTempFile("package-api-test", ".package");
        try {
            PackageMetadata metadata = PackageMetadata.builder()
                .description("Invalid package")
                .language("java").build();
            admin.packages().upload(metadata, "function://public/default/test@v1", file.getPath());
        } catch (PulsarAdminException e) {
            assertEquals(412, e.getStatusCode());
        }

        try {
            PackageMetadata metadata = PackageMetadata.builder()
                .description("Invalid package")
                .functionClassname("Test").build();
            admin.packages().upload(metadata, "function://public/default/test@v1", file.getPath());
        } catch (PulsarAdminException e) {
            assertEquals(412, e.getStatusCode());
        }

        try {
            PackageMetadata metadata = PackageMetadata.builder()
                .description("Invalid package").build();
            admin.packages().upload(metadata, "function://public/default/test@v1", file.getPath());
        } catch (PulsarAdminException e) {
            assertEquals(412, e.getStatusCode());
        }

        // download a non-existent package should return not found exception
        String unknownPackageName = "function://public/default/unknown@v1";
        try {
            admin.packages().download(unknownPackageName, "/test/unknown");
        } catch (PulsarAdminException e) {
            assertEquals(404, e.getStatusCode());
        }

        // get the metadata of a non-existent package should return not found exception
        try {
            admin.packages().getMetadata(unknownPackageName);
        } catch (PulsarAdminException e) {
            assertEquals(404, e.getStatusCode());
        }

        // update the metadata of a non-existent package should return not found exception
        try {
            admin.packages().updateMetadata(unknownPackageName,
                PackageMetadata.builder().description("unknown").build());
        } catch (PulsarAdminException e) {
            assertEquals(404, e.getStatusCode());
        }

        // list all the packages in a non-existent namespace should return not found exception
        try {
            List<String> packagesName = admin.packages().listPackages("function", "unknown/unknown");
            assertEquals(packagesName.size(), 0);
        } catch (PulsarAdminException e) {
            fail("should not throw any exception");
        }

        // list all the versions of a non-existent package should return not found exception
        try {
            List<String> versions = admin.packages().listPackageVersions(unknownPackageName);
            assertEquals(versions.size(), 0);
        } catch (PulsarAdminException e) {
            fail("should not throw any exception");
        }

        // list all the packages with an invalid type should return the precondition failed exception
        try {
            List<String> packagesName = admin.packages().listPackages("invalid", "unknown/unknown");
            fail("should throw precondition exception");
        } catch (PulsarAdminException e) {
            assertEquals(412, e.getStatusCode());
        }
    }
}
