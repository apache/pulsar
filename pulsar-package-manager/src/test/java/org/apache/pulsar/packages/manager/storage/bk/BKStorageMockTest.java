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

package org.apache.pulsar.packages.manager.storage.bk;


import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.testng.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.distributedlog.metadata.LogMetadataStore;
import org.apache.distributedlog.metadata.LogStreamMetadataStore;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BKStorageMockTest {

    private Namespace namespace;
    private DistributedLogManager distributedLogManager;
    private AsyncLogWriter writer;
    private AsyncLogReader reader;

    private final String testData = "test-storage";
    private final List<String> testPath = new ArrayList<>();


    @BeforeMethod
    public void setup() throws Exception {
        distributedLogManager = mock(DistributedLogManager.class);
        when(distributedLogManager.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));

        mockNamespace();
        mockDLInputStream();
        mockDLOutputStream();
    }

    private void mockNamespace() throws Exception {
//        namespace = mock(Namespace.class);
//        NamespaceDriver namespaceDriver = mock(NamespaceDriver.class);
//        LogMetadataStore logMetadataStore = mock(LogMetadataStore.class);
//        LogStreamMetadataStore logStreamMetadataStore = mock(LogStreamMetadataStore.class);
//
//        when(namespace.getNamespaceDriver()).thenReturn(namespaceDriver);
//        when(namespaceDriver.getLogMetadataStore()).thenReturn(logMetadataStore);
//        when(namespaceDriver.getLogStreamMetadataStore(eq(NamespaceDriver.Role.WRITER)))
//            .thenReturn(logStreamMetadataStore);
//        when(logStreamMetadataStore.deleteLog(any(), anyString()))
//            .thenReturn(CompletableFuture.completedFuture(null));
//        when(logMetadataStore.getLogLocation(anyString()))
//            .thenReturn(CompletableFuture.completedFuture(Optional.of(URI.create("distributedlog://test"))));
//        when(namespace.openLog(anyString())).thenReturn(distributedLogManager);
//        doNothing().when(namespace).deleteLog(anyString());
//
//        testPath.add("test/list/path1");
//        testPath.add("test/list/path2");
//        when(logMetadataStore.getLogs(anyString())).thenReturn(CompletableFuture.completedFuture(testPath.iterator()));

    }

    private void mockDLOutputStream() {
        writer = mock(AsyncLogWriter.class);

        when(distributedLogManager.openAsyncLogWriter()).thenReturn(CompletableFuture.completedFuture(writer));
        when(writer.write(any(LogRecord.class))).thenReturn(CompletableFuture.completedFuture(DLSN.InitialDLSN));
        when(writer.markEndOfStream()).thenReturn(CompletableFuture.completedFuture(-1L));
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
    }

    private void mockDLInputStream() {
        reader = mock(AsyncLogReader.class);

        when(distributedLogManager.openAsyncLogReader(any(DLSN.class))).thenReturn(CompletableFuture.completedFuture(reader));

        LogRecordWithDLSN logRecordWithDLSN = new LogRecordWithDLSN(DLSN.InitialDLSN, System.currentTimeMillis(), testData.getBytes(), System.currentTimeMillis());

        List<LogRecordWithDLSN> records = new ArrayList<>();
        records.add(logRecordWithDLSN);

        when(reader.readBulk(anyInt())).thenReturn(CompletableFuture.completedFuture(records))
            .thenReturn(FutureUtil.failedFuture(new EndOfStreamException("eos")));
        when(reader.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterMethod
    public void teardown() {
        if (namespace != null) {
            namespace.close();
        }
    }

    @Test
    public void testWrite() throws Exception{
        BKPackageStorage bkStorage = new BKPackageStorage(namespace);
        String testFileString = "test-file-string";
        ByteArrayInputStream input = new ByteArrayInputStream(testFileString.getBytes());
        bkStorage.writeAsync("test/write-path", input).get();

        verify(writer, times(1)).write(any(LogRecord.class));
        verify(writer, times(1)).markEndOfStream();
        verify(writer, times(1)).asyncClose();
        verify(distributedLogManager, times(1)).asyncClose();
    }

    @Test
    public void testRead() throws Exception {
        BKPackageStorage bkStorage = new BKPackageStorage(namespace);
        OutputStream output = new ByteArrayOutputStream();
        bkStorage.readAsync("test/read-path", output).get();

        verify(reader, times(2)).readBulk(eq(10));
        verify(reader, times(1)).asyncClose();
        verify(distributedLogManager, times(1)).asyncClose();
    }

    @Test
    public void testDelete() {
        BKPackageStorage bkStorage = new BKPackageStorage(namespace);
        try {
            bkStorage.deleteAsync("test/delete-path").get();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testList() throws Exception {
        BKPackageStorage bkStorage = new BKPackageStorage(namespace);
        List<String> paths = bkStorage.listAsync("test/list-path").get();

        assertEquals(paths.size(), testPath.size());
        assertTrue(paths.containsAll(testPath));
    }

    @Test
    public void testExist() throws Exception {
//        URI mockURI = URI.create("distributedlog://test");
//        LogMetadataStore logMetadataStore = mock(LogMetadataStore.class);
//        NamespaceDriver namespaceDriver = mock(NamespaceDriver.class);
//        LogStreamMetadataStore logStreamMetadataStore = mock(LogStreamMetadataStore.class);
//
//        when(namespace.getNamespaceDriver()).thenReturn(namespaceDriver);
//        when(namespaceDriver.getLogMetadataStore()).thenReturn(logMetadataStore);
//        when(logMetadataStore.getLogLocation(eq("test/exist/true")))
//            .thenReturn(CompletableFuture.completedFuture(Optional.of(mockURI)));
//        when(logMetadataStore.getLogLocation(eq("test/exist/false")))
//            .thenReturn(CompletableFuture.completedFuture(Optional.fromNullable(null)));
//        when(namespaceDriver.getLogStreamMetadataStore(eq(NamespaceDriver.Role.WRITER)))
//            .thenReturn(logStreamMetadataStore);
//        when(logStreamMetadataStore.logExists(any(URI.class), eq("test/exist/true")))
//            .thenReturn(CompletableFuture.completedFuture(null));
//
//        BKPackageStorage bkStorage = new BKPackageStorage(namespace);
//        Boolean exists = bkStorage.existAsync("test/exist/true").get();
//        assertTrue(exists);
//
//        exists = bkStorage.existAsync("test/exist/false").get();
//        assertFalse(exists);
    }

}

