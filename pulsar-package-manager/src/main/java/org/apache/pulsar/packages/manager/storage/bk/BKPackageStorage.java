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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.PackageStorageConfig;
import org.apache.pulsar.packages.manager.exception.PackageException;
import org.apache.zookeeper.KeeperException;

/**
 * Using bookKeeper to store the package and package metadata.
 */
@Slf4j
public class BKPackageStorage implements PackageStorage {

    @VisibleForTesting
    Namespace namespace;
    private BKPackageStorageConfig config;

    BKPackageStorage(PackageStorageConfig configuration) {
        this(BKPackageStorageConfig.loadFromPackageConfiguration(configuration));
    }

    BKPackageStorage(BKPackageStorageConfig config) {
        this.config = config;
        setup();
    }

    @VisibleForTesting
    public BKPackageStorage(Namespace namespace) {
        this.namespace = namespace;
    }

    private void setup() {
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setImmediateFlushEnabled(true)
            .setOutputBufferSize(0)
            .setPeriodicFlushFrequencyMilliSeconds(0)
            .setWriteQuorumSize(1)
            .setEnsembleSize(1)
            .setAckQuorumSize(1)
            .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);

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
                .uri(initializeDlogNamespace())
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private URI initializeDlogNamespace() throws IOException {
        BKDLConfig bkdlConfig = new BKDLConfig(config.getZkServers(), config.getLedgersRootPath());
        DLMetadata dlMetadata = DLMetadata.create(bkdlConfig);
        URI dlogURI = URI.create(String.format("distributedlog://%s/pulsar/packages", config.getZkServers()));
        try {
            dlMetadata.create(dlogURI);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == KeeperException.Code.NODEEXISTS) {
                return dlogURI;
            }
            throw e;
        }
        return dlogURI;
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
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        return openLogAsync(path)
            .thenCompose(DLInputStream::openReaderAsync)
            .thenCompose(dlInputStream -> dlInputStream.readAsync(outputStream))
            .thenCompose(DLInputStream::closeAsync);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        return namespace.getNamespaceDriver().getLogMetadataStore().getLogLocation(path)
            .thenCompose(uri -> uri.isPresent()
                ? namespace.getNamespaceDriver()
                    .getLogStreamMetadataStore(NamespaceDriver.Role.WRITER).deleteLog(uri.get(), path)
                : FutureUtil.failedFuture(
                    new PackageException("There is no distributedLog uri when deleting the path " + path)));
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        return namespace.getNamespaceDriver().getLogMetadataStore().getLogs(path)
            .thenApply(logs -> {
                ArrayList<String> packages = new ArrayList<>();
                logs.forEachRemaining(packages::add);
                return packages;
            });
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        namespace.getNamespaceDriver().getLogMetadataStore().getLogLocation(path)
            .whenComplete((uriOptional, throwable) -> {
                if (throwable != null) {
                    result.complete(false);
                    return;
                }

                if (uriOptional.isPresent()) {
                    namespace.getNamespaceDriver()
                        .getLogStreamMetadataStore(NamespaceDriver.Role.WRITER)
                        .logExists(uriOptional.get(), path)
                        .whenComplete((ignore, e) -> {
                            if (e != null) {
                                result.complete(false);
                            } else {
                                result.complete(true);
                            }
                        });
                } else {
                    result.complete(false);
                }
            });
        return result;
    }

    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.runAsync(() -> this.namespace.close());
    }
}
