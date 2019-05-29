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
package org.apache.bookkeeper.mledger.offload.filesystem;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.offload.filesystem.impl.FileSystemManagedLedgerOffloader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FileStoreTestBase {
    private static final Logger log = LoggerFactory.getLogger(FileStoreTestBase.class);
    protected FileSystemManagedLedgerOffloader fileSystemManagedLedgerOffloader;
    protected OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build();
    protected final String basePath = "test_hdfs";
    private MiniDFSCluster hdfsCluster;
    private String hdfsURI;
    private static final String READ_BUFFER_SIZE = "100";

    @BeforeMethod
    public void start() throws Exception {
        File baseDir = Files.createTempDirectory(basePath).toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        FileSystemLedgerOffloaderFactory factory = new FileSystemLedgerOffloaderFactory();
        Properties properties = new Properties();
        properties.put("hdfsFileSystemManagedLedgerOffloadAccessUri", hdfsURI);
        properties.put("hdfsFileSystemManagedLedgerOffloadStorageBasePath", basePath);
        properties.put("managedLedgerOffloadDriver", "hdfs");
        properties.put("hdfsFileSystemReadHandleReadBufferSize", READ_BUFFER_SIZE);
        Map<String, String> map = new HashMap<>();
        fileSystemManagedLedgerOffloader = factory.create(properties, map,OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build());
    }

    @AfterMethod
    public void tearDown() {
        hdfsCluster.shutdown(true, true);
        hdfsCluster.close();
    }

    public String getURI() {
        return hdfsURI;
    }

    public long getReadBufferSize() {
        return Long.parseLong(READ_BUFFER_SIZE);
    }
}
