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

package org.apache.pulsar.packages.manager.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.gson.Gson;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.exception.PackageAlreadyExistsException;
import org.apache.pulsar.packages.manager.exception.PackageMetaAlreadyExistsException;
import org.apache.pulsar.packages.manager.exception.PackageMetaNotFoundException;
import org.apache.pulsar.packages.manager.exception.PackageNotFoundException;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PackagesImplMockTest {

    private PackageImpl packageImpl;
    private PackageStorage packageStorage;
    private final PackageName notFoundName = PackageName.get("function://public/default/not-found@latest");
    private final PackageName canBeFoundName = PackageName.get("function://public/default/found@latest");
    private final static Gson gson = new Gson();

    @BeforeMethod
    private void setup() throws IOException {
        this.packageStorage = mock(PackageStorage.class);
        this.packageImpl = new PackageImpl(packageStorage);

        when(packageStorage.existAsync(eq(getMetadataStoragePath(notFoundName))))
            .thenReturn(CompletableFuture.completedFuture(false));
        when(packageStorage.existAsync(eq(getPackageStoragePath(notFoundName))))
            .thenReturn(CompletableFuture.completedFuture(false));
        when(packageStorage.existAsync(eq(getMetadataStoragePath(canBeFoundName))))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(packageStorage.existAsync(eq(getPackageStoragePath(canBeFoundName))))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(packageStorage.existAsync(eq(getPackagePathWithoutVersion(notFoundName))))
            .thenReturn(CompletableFuture.completedFuture(false));
        when(packageStorage.existAsync(eq(getPackagePathWithoutVersion(canBeFoundName))))
            .thenReturn(CompletableFuture.completedFuture(true));

        List<String> paths = new ArrayList<>();
        paths.add("v1");
        paths.add("v2");
        paths.add("v3");

        when(packageStorage.listAsync(eq(getPackagePathWithoutVersion(canBeFoundName))))
            .thenReturn(CompletableFuture.completedFuture(paths));

        List<String> functionPackages = new ArrayList<>();
        functionPackages.add("function-package-1");
        functionPackages.add("function-package-2");
        functionPackages.add("function-package-3");

        List<String> sourcePackages = new ArrayList<>();
        sourcePackages.add("source-package-1");
        sourcePackages.add("source-package-2");
        sourcePackages.add("source-package-3");

        List<String> sinkPackages = new ArrayList<>();
        sinkPackages.add("sink-package-1");
        sinkPackages.add("sink-package-2");
        sinkPackages.add("sink-package-3");

        when(packageStorage.existAsync(eq("function/public/default")))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(packageStorage.existAsync(eq("source/public/default")))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(packageStorage.existAsync(eq("sink/public/default")))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(packageStorage.listAsync(eq("function/public/default")))
            .thenReturn(CompletableFuture.completedFuture(functionPackages));
        when(packageStorage.listAsync(eq("source/public/default")))
            .thenReturn(CompletableFuture.completedFuture(sourcePackages));
        when(packageStorage.listAsync(eq("sink/public/default")))
            .thenReturn(CompletableFuture.completedFuture(sinkPackages));
    }

    private void mockPackageStorageReadAsync(String path, byte[] mockData) {
        when(packageStorage.readAsync(eq(path), any(OutputStream.class)))
            .then(invocationOnMock -> {
                OutputStream outputStream = invocationOnMock.getArgument(1);
                outputStream.write(mockData, 0, mockData.length);
                return CompletableFuture.completedFuture(null);
            });
    }

    @Test
    public void testGetMeta() throws ExecutionException, InterruptedException {
        // mock package storage read method
        PackageMetadata mockMeta = PackageMetadata.builder()
            .contact("test")
            .description("mock-test").build();
        byte[] mockData = gson.toJson(mockMeta).getBytes(StandardCharsets.UTF_8);
        mockPackageStorageReadAsync(getMetadataStoragePath(canBeFoundName), mockData);

        // test code
        PackageMetadata metadata = packageImpl.getMeta(canBeFoundName).get();
        assertEquals("test", metadata.getContact());
        assertEquals("mock-test", metadata.getDescription());
    }

    @Test
    public void testUpdateMeta() throws ExecutionException, InterruptedException {
        // mock package storage write method
        PackageMetadata mockMeta = PackageMetadata.builder()
            .contact("test-write-contact")
            .description("test-write-description").build();
        when(packageStorage.deleteAsync(eq(getMetadataStoragePath(canBeFoundName))))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(packageStorage.writeAsync(eq(getMetadataStoragePath(canBeFoundName)), any(InputStream.class)))
            .then(invocationOnMock -> {
                InputStream inputStream = invocationOnMock.getArgument(1);
                byte[] buffer = new byte[128];
                int read = inputStream.read(buffer);
                String data = new String(buffer, 0, read, StandardCharsets.UTF_8);
                PackageMetadata metadata = new Gson().fromJson(data, PackageMetadata.class);
                assertEquals("test-write-contact", metadata.getContact());
                assertEquals("test-write-description", metadata.getDescription());
                return CompletableFuture.completedFuture(null);
            });

        // test code
        packageImpl.updateMeta(canBeFoundName, mockMeta).get();
    }

    @Test
    public void testGetMetaFailed() {
        try {
            packageImpl.getMeta(notFoundName).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageMetaNotFoundException);
        }
    }

    @Test
    public void testDownload() throws ExecutionException, InterruptedException {
        // mock package storage read method
        byte[] mockPackageData = "test-package-data".getBytes(StandardCharsets.UTF_8);
        mockPackageStorageReadAsync(getPackageStoragePath(canBeFoundName), mockPackageData);

        // test code
        ByteArrayOutputStream packageData = new ByteArrayOutputStream();
        packageImpl.download(canBeFoundName, packageData).get();
        assertEquals("test-package-data", packageData.toString());
    }

    @Test
    public void testDownloadFailed() {
        try {
            packageImpl.download(notFoundName, new ByteArrayOutputStream()).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageNotFoundException);
        }
    }

    @Test
    public void testUpload() throws ExecutionException, InterruptedException {
        // mock package storage write method
        PackageMetadata mockMeta = PackageMetadata.builder()
            .contact("test-write-contact")
            .description("test-write-description").build();
        when(packageStorage.writeAsync(eq(getMetadataStoragePath(notFoundName)), any(InputStream.class)))
            .then(invocationOnMock -> {
                InputStream inputStream = invocationOnMock.getArgument(1);
                byte[] buffer = new byte[128];
                int read = inputStream.read(buffer);
                String data = new String(buffer, 0, read, StandardCharsets.UTF_8);
                PackageMetadata metadata = new Gson().fromJson(data, PackageMetadata.class);
                assertEquals("test-write-contact", metadata.getContact());
                assertEquals("test-write-description", metadata.getDescription());
                return CompletableFuture.completedFuture(null);
            });

        byte[] testPackageData = "test-package-data".getBytes(StandardCharsets.UTF_8);
        when(packageStorage.writeAsync(eq(getPackageStoragePath(notFoundName)), any(InputStream.class)))
            .then(invocationOnMock -> {
                InputStream inputStream = invocationOnMock.getArgument(1);
                byte[] buffer = new byte[64];
                int read = inputStream.read(buffer);
                String data = new String(buffer, 0, read);
                assertEquals("test-package-data", data);
                return CompletableFuture.completedFuture(null);
            });

        // test code
        packageImpl.upload(notFoundName, mockMeta, new ByteArrayInputStream(testPackageData)).get();
    }

    @Test
    public void testUploadFailed() {
        try {
            packageImpl.upload(canBeFoundName, PackageMetadata.builder().build(), new ByteArrayInputStream("".getBytes())).get();
            fail("should throw package meta already exist exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageMetaAlreadyExistsException);
        }

        when(packageStorage.existAsync(anyString()))
            .thenReturn(CompletableFuture.completedFuture(false))
            .thenReturn(CompletableFuture.completedFuture(true));

        try {
            packageImpl.upload(canBeFoundName, PackageMetadata.builder().build(), new ByteArrayInputStream("".getBytes())).get();
            fail("should throw package already exist exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PackageAlreadyExistsException);
        }
    }

    @Test
    public void testListAllVersionOfPackages() throws ExecutionException, InterruptedException {
        List<PackageName> packages = packageImpl.list(canBeFoundName).get();
        assertEquals(3, packages.size());
        assertTrue(packages.contains(PackageName.get(canBeFoundName.getPkgType().toString(), canBeFoundName.getCompleteName(), "v1")));
        assertTrue(packages.contains(PackageName.get(canBeFoundName.getPkgType().toString(), canBeFoundName.getCompleteName(), "v2")));
        assertTrue(packages.contains(PackageName.get(canBeFoundName.getPkgType().toString(), canBeFoundName.getCompleteName(), "v3")));
    }

    @Test
    public void testListAllPackagesUnderNamespaces() throws ExecutionException, InterruptedException {
//        List<PackageName> functions = packageImpl.list(PackageType.FUNCTION, canBeFoundName.getNamespaceName()).get();
//        assertEquals(3, functions.size());
//        assertTrue(functions.contains(PackageName.get(PackageType.FUNCTION.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/function-package-1")));
//        assertTrue(functions.contains(PackageName.get(PackageType.FUNCTION.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/function-package-2")));
//        assertTrue(functions.contains(PackageName.get(PackageType.FUNCTION.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/function-package-3")));
//
//        List<PackageName> sources = packageImpl.list(PackageType.SOURCE, canBeFoundName.getNamespaceName()).get();
//        assertEquals(3, sources.size());
//        assertTrue(sources.contains(PackageName.get(PackageType.SOURCE.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/source-package-1")));
//        assertTrue(sources.contains(PackageName.get(PackageType.SOURCE.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/source-package-2")));
//        assertTrue(sources.contains(PackageName.get(PackageType.SOURCE.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/source-package-3")));
//
//        List<PackageName> sinks = packageImpl.list(PackageType.SINK, canBeFoundName.getNamespaceName()).get();
//        assertEquals(3, sinks.size());
//        assertTrue(sinks.contains(PackageName.get(PackageType.SINK.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/sink-package-1")));
//        assertTrue(sinks.contains(PackageName.get(PackageType.SINK.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/sink-package-2")));
//        assertTrue(sinks.contains(PackageName.get(PackageType.SINK.toString() + "://" + canBeFoundName.getNamespaceName().toString() + "/sink-package-3")));
    }

    private static String getMetadataStoragePath(PackageName packageName) {
        return String.format("%s/%s", getPackageStoragePath(packageName), "meta");
    }

    private static String getPackageStoragePath(PackageName packageName) {
        return String.format("%s/%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getTenant(),
            packageName.getNamespace(),
            packageName.getName(),
            packageName.getVersion());
    }

    private static String getPackagePathWithoutVersion(PackageName packageName) {
        return String.format("%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getTenant(),
            packageName.getNamespace(),
            packageName.getName());
    }

}
