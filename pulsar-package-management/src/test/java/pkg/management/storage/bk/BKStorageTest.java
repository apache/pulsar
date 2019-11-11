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

package pkg.management.storage.bk;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import org.apache.distributedlog.AppendOnlyStreamReader;
import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pkg.management.PkgStorageConfig;

public class BKStorageTest {

    private final static String testFileString = "test-file-string";
    private BKStorage bkStorage;
    private Namespace namespace;

    private DistributedLogManager distributedLogManager;
    private AppendOnlyStreamWriter writer;
    private AppendOnlyStreamReader reader;
    private LogReader logReader;


    @BeforeMethod
    public void setup() throws Exception {
        this.namespace = mock(Namespace.class);
        this.bkStorage = mock(BKStorage.class);
        this.distributedLogManager = mock(DistributedLogManager.class);
        this.writer = mock(AppendOnlyStreamWriter.class);
        this.reader = mock(AppendOnlyStreamReader.class);
        this.logReader = mock(LogReader.class);

        whenNew(BKStorage.class).withParameterTypes(PkgStorageConfig.class).withArguments(any(PkgStorageConfig.class))
                                .thenReturn(bkStorage);
        this.bkStorage = new BKStorage(namespace);
        when(namespace.openLog(any(String.class))).thenReturn(distributedLogManager);
        when(distributedLogManager.getAppendOnlyStreamWriter()).thenReturn(writer);
        when(distributedLogManager.getAppendOnlyStreamReader()).thenReturn(reader);
        when(distributedLogManager.getInputStream(eq(DLSN.InitialDLSN))).thenReturn(logReader);
    }

    @Test
    public void testWrite() throws Exception{
        ByteArrayInputStream input = new ByteArrayInputStream(testFileString.getBytes());
        bkStorage.write("/test/write-path", input).get();
        verify(namespace, times(1)).openLog(eq("/test/write-path"));
        verify(distributedLogManager, times(1)).getAppendOnlyStreamWriter();
        verify(writer, times(1)).markEndOfStream();
        verify(writer, times(1)).close();
        verify(distributedLogManager, times(1)).close();
    }

    @Test
    public void testWriteBytes() throws Exception {
        bkStorage.write("/test/write-path", testFileString.getBytes()).get();
        verify(namespace, times(1)).openLog(eq("/test/write-path"));
        verify(distributedLogManager, times(1)).getAppendOnlyStreamWriter();
        verify(writer, times(1)).write(eq(testFileString.getBytes()));
        verify(writer, times(1)).force(eq(false));
        verify(distributedLogManager, times(1)).close();
    }

    @Test
    public void testRead() throws Exception {
        Mockito.when(logReader.readNext(anyBoolean())).thenThrow(new EndOfStreamException("eos"));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        bkStorage.read("/test/read-path", output).get();
        verify(namespace, times(1)).openLog(eq("/test/read-path"));
        verify(distributedLogManager, times(1)).getInputStream(eq(DLSN.InitialDLSN));
        verify(distributedLogManager, times(1)).close();
    }

    @Test
    public void testReadBytes() throws Exception {
        bkStorage.read("/test/read-path").get();
        verify(namespace, times(1)).openLog(eq("/test/read-path"));
        verify(distributedLogManager, times(1)).getAppendOnlyStreamReader();
        verify(reader, times(1)).available();
        verify(reader, times(1)).read(any());
        verify(reader, times(1)).close();
        verify(distributedLogManager, times(1)).close();
    }

    @Test
    public void testDelete() throws Exception {
        bkStorage.delete("/test/delete-path").get();
        verify(namespace, times(1)).deleteLog(eq("/test/delete-path"));
    }

    @Test
    public void testList() throws Exception {
        ArrayList<String> paths = new ArrayList<>();
        paths.add("/test/list-path-one");
        paths.add("/test/list-path-two");
        when(namespace.getLogs(eq("/test/list-path"))).thenReturn(paths.iterator());
        bkStorage.list("/test/list-path").get();
        verify(namespace, times(1)).getLogs(eq("/test/list-path"));
    }

    @Test
    public void testExist() throws Exception {
        bkStorage.exist("/test/exist-path").get();
        verify(namespace, times(1)).logExists(eq("/test/exist-path"));
    }

}

