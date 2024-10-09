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
 */
package org.apache.bookkeeper.mledger.offload.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.offload.filesystem.impl.FileSystemManagedLedgerOffloader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class FileStoreTestBase {
    protected FileSystemManagedLedgerOffloader fileSystemManagedLedgerOffloader;
    protected OrderedScheduler scheduler;
    protected final String basePath = "pulsar";
    private MiniDFSCluster hdfsCluster;
    private String hdfsURI;
    protected LedgerOffloaderStats offloaderStats;
    private ScheduledExecutorService scheduledExecutorService;

    @BeforeClass(alwaysRun = true)
    public final void beforeClass() throws Exception {
        init();
    }

    public void init() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build();
    }

    @AfterClass(alwaysRun = true)
    public final void afterClass() throws IOException {
        cleanup();
    }

    public void cleanup() throws IOException {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void start() throws Exception {
        File baseDir = Files.createTempDirectory(basePath).toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.set("dfs.namenode.gc.time.monitor.enable", "false");
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        Properties properties = new Properties();
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        this.offloaderStats = LedgerOffloaderStats.create(true, true, scheduledExecutorService, 60);
        fileSystemManagedLedgerOffloader = new FileSystemManagedLedgerOffloader(
                OffloadPoliciesImpl.create(properties),
                scheduler, hdfsURI, basePath, offloaderStats);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (fileSystemManagedLedgerOffloader != null) {
            fileSystemManagedLedgerOffloader.close();
            fileSystemManagedLedgerOffloader = null;
        }
        if (hdfsCluster != null) {
            hdfsCluster.shutdown(true, true);
            hdfsCluster = null;
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
        }
    }

    public String getURI() {
        return hdfsURI;
    }
}
