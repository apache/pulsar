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
package org.apache.pulsar.transaction.coordinator.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.StateStores;

import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVAsyncStore;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVStore;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.transaction.common.utils.CoordinatorUtils;
import org.apache.pulsar.transaction.configuration.CoordinatorConfiguration;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorMetadataStoreException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * A provider provides {@link TransactionMetadataStore} that will persist
 * meta data to durable storage and able to recover from machine failure.
 */
@RequiredArgsConstructor
@Slf4j
public class PersistentTransactionMetadataStoreProvider implements TransactionMetadataStoreProvider {

    @NonNull
    PulsarAdmin pulsarAdmin;

    @NonNull
    CoordinatorConfiguration coordinatorConfiguration;

    @Override
    public CompletableFuture<TransactionMetadataStore> openStore(TransactionCoordinatorID transactionCoordinatorId) {
        CompletableFuture<TransactionMetadataStore> future = new CompletableFuture<>();
        InternalConfigurationData internalConf;
        try {
            internalConf = pulsarAdmin.brokers().getInternalConfigurationData();
            URI dlNameSpace = CoordinatorUtils.initializeDLNamespace(internalConf.getZookeeperServers(),
                                                                     internalConf.getLedgersRootPath());

            DistributedLogConfiguration dlConf = CoordinatorUtils.getDLConf(coordinatorConfiguration);

            Namespace namespace = NamespaceBuilder.newBuilder()
                    .conf(dlConf)
                    .clientId("transaction-coordinator" + transactionCoordinatorId)
                    .uri(dlNameSpace)
                    .build();
            future.complete(new PersistentTransactionMetadataStore(transactionCoordinatorId, new RocksdbKVAsyncStore<>(
                                                                                    () -> new RocksdbKVStore<>(),
                                                                                    () -> namespace)));
        } catch (IOException e) {
            log.error("Exception open transaction meta store:", e);
            future.completeExceptionally(new CoordinatorMetadataStoreException(e));
        } catch (PulsarAdminException e) {
            log.error("Exception open transaction meta store:", e);
            future.completeExceptionally(new CoordinatorMetadataStoreException(e));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return future;
    }
}
