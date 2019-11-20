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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.PackageStorageConfig;

/**
 * Using bookKeeper to store the package and package metadata.
 */
public class BKPackageStorage implements PackageStorage {

    private Namespace namespace;
    private BKPackageStorageConfig config;

    BKPackageStorage(PackageStorageConfig config) {
        this.config = (BKPackageStorageConfig) config;
        setup();
    }

    @VisibleForTesting
    public BKPackageStorage(Namespace namespace) {
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

    private CompletableFuture<DistributedLogManager> openLogAsync(String path) {
        CompletableFuture<DistributedLogManager> logFuture = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                logFuture.complete(namespace.openLog(path));
            } catch (IOException e) {
                logFuture.completeExceptionally(e);
            }
        });
        return logFuture;
    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, InputStream inputStream) {
        return openLogAsync(path)
            .thenCompose(DLOutputStream::openWriterAsync)
            .thenCompose(dlOutputStream -> dlOutputStream.writeAsync(inputStream))
            .thenCompose(DLOutputStream::closeAsync);
    }


    @Override
    public CompletableFuture<Void> writeAsync(String path, byte[] data) {
        return openLogAsync(path)
            .thenCompose(DLOutputStream::openWriterAsync)
            .thenCompose(dlOutputStream -> dlOutputStream.writeAsync(data))
            .thenCompose(DLOutputStream::closeAsync);
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        return openLogAsync(path)
            .thenCompose(DLInputStream::openReaderAsync)
            .thenCompose(dlInputStream -> dlInputStream.readAsync(outputStream))
            .thenCompose(DLInputStream::closeAsync);
    }

    @Override
    public CompletableFuture<byte[]> readAsync(String path) {
        return openLogAsync(path)
            .thenCompose(DLInputStream::openReaderAsync)
            .thenCompose(DLInputStream::readAsync)
            .thenCompose(DLInputStream.ByteResult::getResult);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                namespace.deleteLog(path);
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                future.complete(listSync(path));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private List<String> listSync(String path) throws IOException {
        List<String> paths = new ArrayList<>();
        namespace.getLogs(path).forEachRemaining(paths::add);
        return paths;
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                future.complete(namespace.logExists(path));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.runAsync(() -> this.namespace.close());
    }
}
