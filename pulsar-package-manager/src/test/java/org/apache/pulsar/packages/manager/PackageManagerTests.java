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
package org.apache.pulsar.packages.manager;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.pulsar.packages.manager.exceptions.PackageManagerException;
import org.apache.pulsar.packages.manager.impl.PackageManagerImpl;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.storage.memory.MemoryPackageStorageConfig;
import org.apache.pulsar.packages.manager.storage.memory.MemoryPackageStorageProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class PackageManagerTests {

    @DataProvider(name = "providers")
    public static Object[][] storageProviders() {
        return new Object[][]{
            {
                MemoryPackageStorageProvider.class.getName(),
                MemoryPackageStorageConfig.builder().dataMaxSize(10 * 1024 * 1024).build()
            }
        };
    }

    private final PackageStorageProvider provider;
    private final PackageStorageConfig storageConfig;
    private PackageManager packageManager;

    @Factory(dataProvider = "providers")
    public PackageManagerTests(String providerClassName, PackageStorageConfig storageConfig) throws IOException {
        this.storageConfig = storageConfig;
        this.provider = PackageStorageProvider.newProvider(providerClassName);
    }

    @BeforeMethod
    public void setup() throws ExecutionException, InterruptedException {
        this.packageManager = new PackageManagerImpl(this.provider.getStorage(storageConfig).get());
    }

    @Test
    public void testGetPackageMetadataNotFound() {
        try {
            packageManager.getMetadata(PackageName.get("function://public/default/f1@v1")).get();
            fail("should fail to get metadata of a non-existent package");
        } catch (Exception e) {
            assertTrue(e.getCause().getCause() instanceof PackageManagerException.MetadataNotFoundException);
        }
    }

    @Test
    public void testDownloadPackageNotFound() {
    }
}
