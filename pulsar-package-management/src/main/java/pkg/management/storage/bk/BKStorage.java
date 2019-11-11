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

package pkg.management.storage.bk;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.AppendOnlyStreamReader;
import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import pkg.management.PkgStorage;
import pkg.management.PkgStorageConfig;

// Using bookKeeper to store the package
@Slf4j
public class BKStorage implements PkgStorage {

    private Namespace namespace;
    private BKStorageConfig config;

    public BKStorage(PkgStorageConfig config) {
        this.config = (BKStorageConfig) config;
        setup();
    }

    @VisibleForTesting
    public BKStorage(Namespace namespace) {
        this.namespace = namespace;
    }

    private void setup() {
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                                               .setWriteLockEnabled(false)
                                               .setOutputBufferSize(256 * 1024)                  // 256k
                                               .setPeriodicFlushFrequencyMilliSeconds(0)         // disable periodical flush
                                               .setImmediateFlushEnabled(false)                  // disable immediate flush
                                               .setLogSegmentRollingIntervalMinutes(0)           // disable time-based rolling
                                               .setMaxLogSegmentBytes(Long.MAX_VALUE)            // disable size-based rolling
                                               .setExplicitTruncationByApplication(true)         // no auto-truncation
                                               .setRetentionPeriodHours(Integer.MAX_VALUE)       // long retention
                                               .setEnsembleSize(config.numReplicas)                     // replica settings
                                               .setWriteQuorumSize(config.numReplicas)
                                               .setAckQuorumSize(config.numReplicas)
                                               .setUseDaemonThread(true);

        conf.setProperty("bkc.allowShadedLedgerManagerFactoryClass", true);
        conf.setProperty("bkc.shadedLedgerManagerFactoryClassPrefix", "dlshade.");
        if (StringUtils.isNotBlank(config.getBookkeeperClientAuthenticationPlugin())) {
            conf.setProperty("bkc.clientAuthProviderFactoryClass", config.getBookkeeperClientAuthenticationPlugin());
            if (StringUtils.isNotBlank(config.getBookkeeperClientAuthenticationParametersName())) {
                conf.setProperty("bkc." + config.getBookkeeperClientAuthenticationParametersName(),
                                 config.getBookkeeperClientAuthenticationParameters());
            }
        }
        try {
            this.namespace = NamespaceBuilder.newBuilder()
                                             .conf(conf)
                                             .clientId("package-management")
                                             .uri(config.url)
                                             .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> write(String path, InputStream inputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            DistributedLogManager distributedLogManager = namespace.openLog(path);
            AppendOnlyStreamWriter writer = distributedLogManager.getAppendOnlyStreamWriter();
            try (OutputStream outputStream = new DLOutputStream(distributedLogManager, writer)) {
                int read = 0;
                byte[] bytes = new byte[1024];
                while ((read = inputStream.read(bytes)) != -1) {
                    outputStream.write(bytes, 0, read);
                }
                outputStream.flush();
            }
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] data) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try (DistributedLogManager dlm = namespace.openLog(path)) {
            try (AppendOnlyStreamWriter writer = dlm.getAppendOnlyStreamWriter()) {
                writer.write(data);
                writer.force(false);
                future.complete(null);
            }
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> read(String path, OutputStream outputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            DistributedLogManager distributedLogManager = namespace.openLog(path);
            try (InputStream inputStream = new DLInputStream(distributedLogManager)) {
                int read = 0;
                byte[] bytes = new byte[1024];
                while ((read = inputStream.read(bytes)) != -1) {
                    outputStream.write(bytes, 0, read);
                }
                outputStream.flush();
            }
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<byte[]> read(String path) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();

        try (DistributedLogManager dlm = namespace.openLog(path)) {
            try (AppendOnlyStreamReader reader = dlm.getAppendOnlyStreamReader()) {
                byte[] bytes = new byte[reader.available()];
                reader.read(bytes);
                future.complete(bytes);
            }
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            namespace.deleteLog(path);
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<List<String>> list(String path) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();

        try {
            List<String> paths = new ArrayList<>();
            namespace.getLogs(path).forEachRemaining(paths::add);
            future.complete(paths);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> exist(String path) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        try {
            if (namespace.logExists(path)) {
                future.complete(true);
            }
            future.complete(false);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    public CompletableFuture<Void> close() {
        this.namespace.close();
        return CompletableFuture.completedFuture(null);
    }
}
