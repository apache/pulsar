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
package org.apache.pulsar.packages.management.storage.bookkeeper;

import com.google.common.base.Strings;
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
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.PackagesStorageConfiguration;
import org.apache.zookeeper.KeeperException;


/**
 * Packages management storage implementation with bookkeeper.
 */
@Slf4j
public class BookKeeperPackagesStorage implements PackagesStorage {

    private static final String NS_CLIENT_ID = "packages-management";
    final BookKeeperPackagesStorageConfiguration configuration;
    private Namespace namespace;

    BookKeeperPackagesStorage(PackagesStorageConfiguration configuration) {
        this.configuration = new BookKeeperPackagesStorageConfiguration(configuration);
    }

    @Override
    public void initialize() {
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setImmediateFlushEnabled(true)
            .setOutputBufferSize(0)
            .setWriteQuorumSize(configuration.getPackagesReplicas())
            .setEnsembleSize(configuration.getPackagesReplicas())
            .setAckQuorumSize(configuration.getPackagesReplicas())
            .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
        if (!Strings.isNullOrEmpty(configuration.getBookkeeperClientAuthenticationPlugin())) {
            conf.setProperty("bkc.clientAuthProviderFactoryClass",
                configuration.getBookkeeperClientAuthenticationPlugin());
            if (!Strings.isNullOrEmpty(configuration.getBookkeeperClientAuthenticationParametersName())) {
                conf.setProperty("bkc." + configuration.getBookkeeperClientAuthenticationParametersName(),
                    configuration.getBookkeeperClientAuthenticationParameters());
            }
        }
        try {
            this.namespace = NamespaceBuilder.newBuilder()
                .conf(conf).clientId(NS_CLIENT_ID).uri(initializeDlogNamespace()).build();
        } catch (IOException e) {
            throw new RuntimeException("Initialize distributed log for packages management service failed.", e);
        }
        log.info("Packages management bookKeeper storage initialized successfully");
    }

    private URI initializeDlogNamespace() throws IOException {
        String bookkeeperMetadataServiceUri = configuration.getProperty("bookkeeperMetadataServiceUri");
        String ledgersRootPath;
        String ledgersStoreServers;
        if (StringUtils.isNotBlank(bookkeeperMetadataServiceUri)) {
            URI metadataServiceUri = URI.create(bookkeeperMetadataServiceUri);
            ledgersStoreServers = metadataServiceUri.getAuthority().replace(";", ",");
            ledgersRootPath = metadataServiceUri.getPath();
        } else {
            ledgersRootPath = configuration.getPackagesManagementLedgerRootPath();
            ledgersStoreServers = configuration.getZookeeperServers();
        }
        BKDLConfig bkdlConfig = new BKDLConfig(ledgersStoreServers, ledgersRootPath);
        DLMetadata dlMetadata = DLMetadata.create(bkdlConfig);
        URI dlogURI = URI.create(String.format("distributedlog://%s/pulsar/packages",
            configuration.getZookeeperServers()));
        try {
            dlMetadata.create(dlogURI);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == KeeperException.Code.NODEEXISTS) {
                return dlogURI;
            }
        }
        return dlogURI;
    }

    private CompletableFuture<DistributedLogManager> openLogManagerAsync(String path) {
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
        return openLogManagerAsync(path)
            .thenCompose(DLOutputStream::openWriterAsync)
            .thenCompose(dlOutputStream -> dlOutputStream.writeAsync(inputStream))
            .thenCompose(DLOutputStream::closeAsync);
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        return openLogManagerAsync(path)
            .thenCompose(DLInputStream::openReaderAsync)
            .thenCompose(dlInputStream -> dlInputStream.readAsync(outputStream))
            .thenCompose(DLInputStream::closeAsync);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        return namespace.getNamespaceDriver().getLogMetadataStore().getLogLocation(path)
            .thenCompose(uri -> uri.map(value -> namespace.getNamespaceDriver()
                .getLogStreamMetadataStore(NamespaceDriver.Role.WRITER).deleteLog(value, path))
                .orElse(null));
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
        return result;    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.runAsync(() -> this.namespace.close());
    }
}
