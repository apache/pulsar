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
package org.apache.pulsar.broker.loadbalance.extensions.store;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;

/**
 * The load data store, base on {@link TableView <T>}.
 *
 * @param <T> Load data type.
 */
public class TableViewLoadDataStoreImpl<T> implements LoadDataStore<T> {

    private TableView<T> tableView;

    private final Producer<T> producer;

    private final PulsarClient client;

    private final String topic;

    private final Class<T> clazz;

    public TableViewLoadDataStoreImpl(PulsarClient client, String topic, Class<T> clazz) throws LoadDataStoreException {
        try {
            this.client = client;
            this.producer = client.newProducer(Schema.JSON(clazz)).topic(topic).create();
            this.topic = topic;
            this.clazz = clazz;
        } catch (Exception e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public CompletableFuture<Void> pushAsync(String key, T loadData) {
        return producer.newMessage().key(key).value(loadData).sendAsync().thenAccept(__ -> {});
    }

    @Override
    public CompletableFuture<Void> removeAsync(String key) {
        return producer.newMessage().key(key).value(null).sendAsync().thenAccept(__ -> {});
    }

    @Override
    public Optional<T> get(String key) {
        validateTableViewStart();
        return Optional.ofNullable(tableView.get(key));
    }

    @Override
    public void forEach(BiConsumer<String, T> action) {
        validateTableViewStart();
        tableView.forEach(action);
    }

    public Set<Map.Entry<String, T>> entrySet() {
        validateTableViewStart();
        return tableView.entrySet();
    }

    @Override
    public int size() {
        validateTableViewStart();
        return tableView.size();
    }

    @Override
    public void closeTableView() throws IOException {
        if (tableView != null) {
            tableView.close();
            tableView = null;
        }
    }

    @Override
    public void startTableView() throws LoadDataStoreException {
        if (tableView == null) {
            try {
                tableView = client.newTableViewBuilder(Schema.JSON(clazz)).topic(topic).create();
            } catch (PulsarClientException e) {
                tableView = null;
                throw new LoadDataStoreException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
        closeTableView();
    }

    private void validateTableViewStart() {
        if (tableView == null) {
            throw new IllegalStateException("table view has not been started");
        }
    }

}
