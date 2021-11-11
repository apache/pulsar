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
package org.apache.pulsar.functions.instance.state;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.SimpleStorageAdminClientImpl;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.util.Backoff.Jitter;
import org.apache.bookkeeper.common.util.Backoff.Jitter.Type;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.pulsar.functions.api.StateStore;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.FunctionCommon;

/**
 * The state store provider that provides bookkeeper table backed state stores.
 */
@Slf4j
public class BKStateStoreProviderImpl implements StateStoreProvider {

    private String stateStorageServiceUrl;
    private Map<String, StorageClient> clients;

    @Override
    public void init(Map<String, Object> config, FunctionDetails functionDetails) throws Exception {
        stateStorageServiceUrl = (String) config.get(STATE_STORAGE_SERVICE_URL);
        clients = new HashMap<>();
    }

    private StorageClient getStorageClient(String tenant, String namespace) {
        final String tableNs = FunctionCommon.getStateNamespace(tenant, namespace);

        StorageClient client = clients.get(tableNs);
        if (null != client) {
            return client;
        }

        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .serviceUri(stateStorageServiceUrl)
            .enableServerSideRouting(true)
            .clientName("function-" + tableNs)
            // configure a maximum 2 minutes jitter backoff for accessing table service
            .backoffPolicy(Jitter.of(
                Type.EXPONENTIAL,
                100,
                2000,
                60
            ))
            .build();

        StorageClient storageClient = StorageClientBuilder.newBuilder()
                .withSettings(settings)
                .withNamespace(tableNs)
                .build();

        clients.put(tableNs, storageClient);
        return storageClient;
    }

    private void createStateTable(String stateStorageServiceUrl,
                                  String tenant,
                                  String namespace,
                                  String name) throws Exception {
        final String tableNs = FunctionCommon.getStateNamespace(tenant, namespace);
        final String tableName = name;

    	try (StorageAdminClient storageAdminClient = new SimpleStorageAdminClientImpl(
             StorageClientSettings.newBuilder().serviceUri(stateStorageServiceUrl).build(),
             ClientResources.create().scheduler())){
            StreamConfiguration streamConf = StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
                .setInitialNumRanges(4)
                .setMinNumRanges(4)
                .setStorageType(StorageType.TABLE)
                .build();
            Stopwatch elapsedWatch = Stopwatch.createStarted();

            Exception lastException = null;
            while (elapsedWatch.elapsed(TimeUnit.MINUTES) < 1) {
                try {
                    result(storageAdminClient.getStream(tableNs, tableName));
                    return;
                } catch (NamespaceNotFoundException nnfe) {
                    try {
                        result(storageAdminClient.createNamespace(tableNs, NamespaceConfiguration.newBuilder()
                                .setDefaultStreamConf(streamConf)
                                .build()));
                    } catch (Exception e) {
                        // there might be two clients conflicting at creating table, so let's retrieve the table again
                        // to make sure the table is created.
                        lastException = e;
                        log.warn("Encountered exception when creating namespace {} for state table", tableName, e);
                    }
                    try {
                        result(storageAdminClient.createStream(tableNs, tableName, streamConf));
                    } catch (Exception e) {
                        // there might be two clients conflicting at creating table, so let's retrieve the table again
                        // to make sure the table is created.
                        lastException = e;
                        log.warn("Encountered exception when creating table {}/{}", tableNs, tableName, e);
                    }
                } catch (StreamNotFoundException snfe) {
                    try {
                        result(storageAdminClient.createStream(tableNs, tableName, streamConf));
                    } catch (Exception e) {
                        // there might be two client conflicting at creating table, so let's retrieve it to make
                        // sure the table is created.
                        lastException = e;
                        log.warn("Encountered exception when creating table {}/{}", tableNs, tableName, e);
                    }
                } catch (ClientException ce) {
                    log.warn("Encountered issue {} on fetching state stable metadata, re-attempting in 100 milliseconds",
                        ce.getMessage());
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
            throw new IOException(String.format("Failed to setup / verify state table for function %s/%s/%s within timeout", tenant, name, name), lastException);
        }
    }

    private Table<ByteBuf, ByteBuf> openStateTable(String tenant,
                                                   String namespace,
                                                   String name) throws Exception {
        StorageClient client = getStorageClient(tenant, namespace);

        log.info("Opening state table for function {}/{}/{}", tenant, namespace, name);
        // NOTE: this is a workaround until we bump bk version to 4.9.0
        // table might just be created above, so it might not be ready for serving traffic
        Stopwatch openSw = Stopwatch.createStarted();
        while (openSw.elapsed(TimeUnit.MINUTES) < 1) {
            try {
                return result(client.openTable(name), 1, TimeUnit.MINUTES);
            } catch (InternalServerException ise) {
                log.warn("Encountered internal server on opening state table '{}/{}/{}', re-attempt in 100 milliseconds : {}",
                    tenant, namespace, name, ise.getMessage());
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (TimeoutException e) {
                throw new RuntimeException("Failed to open state table for function " + tenant + "/" + namespace + "/" + name + " within timeout period", e);
            }
        }
        throw new IOException("Failed to open state table for function " + tenant + "/" + namespace + "/" + name);
    }

    @Override
    public <S extends StateStore> S getStateStore(String tenant, String namespace, String name) throws Exception {
        // we defer creation of the state table until a java instance is running here.
        createStateTable(stateStorageServiceUrl, tenant, namespace, name);
        Table<ByteBuf, ByteBuf> table = openStateTable(tenant, namespace, name);
        return (S) new BKStateStoreImpl(tenant, namespace, name, table);
    }

    @Override
    public void close() {
        clients.forEach((name, client) -> client.closeAsync()
            .exceptionally(cause -> {
                log.warn("Failed to close state storage client", cause);
                return null;
            })
        );
        clients.clear();
    }
}
