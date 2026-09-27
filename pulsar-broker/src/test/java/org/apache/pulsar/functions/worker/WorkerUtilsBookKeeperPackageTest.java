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
package org.apache.pulsar.functions.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.functions.worker.dlog.DLInputStream;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests uploading and downloading function packages to and from BookKeeper with the DistributedLog namespace that
 * the function worker uses.
 */
@Test(groups = "functions-worker")
public class WorkerUtilsBookKeeperPackageTest {
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(3);

    private LocalBookkeeperEnsemble bkEnsemble;
    private Namespace dlogNamespace;

    @BeforeClass
    void setup() throws Exception {
        bkEnsemble = new LocalBookkeeperEnsemble(1, 0);
        bkEnsemble.start();
        InternalConfigurationData internalConf = new InternalConfigurationData(
                "127.0.0.1:" + bkEnsemble.getZookeeperPort(), null, "/ledgers", null, null);
        URI dlogUri = WorkerUtils.initializeDlogNamespace(internalConf);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setNumFunctionPackageReplicas(1);
        dlogNamespace = NamespaceBuilder.newBuilder()
                .conf(WorkerUtils.getDlogConf(workerConfig))
                .clientId("function-worker-test")
                .uri(dlogUri)
                .build();
    }

    @AfterClass(alwaysRun = true)
    void cleanup() throws Exception {
        if (dlogNamespace != null) {
            dlogNamespace.close();
            dlogNamespace = null;
        }
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
    }

    private byte[] download(String packagePath) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DistributedLogManager dlm = dlogNamespace.openLog(packagePath);
             InputStream in = new DLInputStream(dlm, READ_TIMEOUT)) {
            in.transferTo(out);
        }
        return out.toByteArray();
    }

    @Test
    public void testUploadAndDownload() throws Exception {
        byte[] data = new byte[3_000_000];
        ThreadLocalRandom.current().nextBytes(data);
        WorkerUtils.uploadToBookKeeper(dlogNamespace, new ByteArrayInputStream(data), "tenant/ns/fn/complete");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WorkerUtils.downloadFromBookkeeper(dlogNamespace, out, "tenant/ns/fn/complete");
        assertThat(out.toByteArray()).isEqualTo(data);
    }

    @Test
    public void testDownloadMissingPackageFails() {
        assertThatThrownBy(() -> download("tenant/ns/fn/missing"))
                .isInstanceOf(LogNotFoundException.class);
    }

    @Test
    public void testDownloadDeletedPackageFails() throws Exception {
        WorkerUtils.uploadToBookKeeper(dlogNamespace, new ByteArrayInputStream(new byte[1000]),
                "tenant/ns/fn/deleted");
        WorkerUtils.deleteFromBookkeeper(dlogNamespace, "tenant/ns/fn/deleted");
        assertThatThrownBy(() -> download("tenant/ns/fn/deleted"))
                .isInstanceOf(LogNotFoundException.class);
    }

    /**
     * A log stream that exists but has never been written to has no log segments to read. The download must fail
     * after the read timeout instead of waiting forever for records to be written.
     */
    @Test
    public void testDownloadEmptyPackageTimesOut() throws Exception {
        dlogNamespace.createLog("tenant/ns/fn/empty");
        assertThatThrownBy(() -> download("tenant/ns/fn/empty"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Timed out");
    }

    /**
     * A log stream of an upload that failed before {@link org.apache.pulsar.functions.worker.dlog.DLOutputStream}
     * wrote the end-of-stream marker. The download must fail after the read timeout instead of waiting forever
     * for the marker.
     */
    @Test
    public void testDownloadIncompletePackageTimesOut() throws Exception {
        byte[] data = new byte[1000];
        try (DistributedLogManager dlm = dlogNamespace.openLog("tenant/ns/fn/incomplete");
             var writer = dlm.getAppendOnlyStreamWriter()) {
            writer.write(data);
            writer.force(false);
        }
        assertThatThrownBy(() -> download("tenant/ns/fn/incomplete"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Timed out");
    }
}
