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
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
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
@Slf4j
public class TableViewLoadDataStoreImpl<T> implements LoadDataStore<T> {

    private static final long LOAD_DATA_REPORT_UPDATE_MAX_INTERVAL_MULTIPLIER_BEFORE_RESTART = 2;

    private volatile TableView<T> tableView;
    private volatile long tableViewLastUpdateTimestamp;

    private volatile Producer<T> producer;

    private final ServiceConfiguration conf;

    private final PulsarClient client;

    private final String topic;

    private final Class<T> clazz;

    public TableViewLoadDataStoreImpl(PulsarService pulsar, String topic, Class<T> clazz)
            throws LoadDataStoreException {
        try {
            this.conf = pulsar.getConfiguration();
            this.client = pulsar.getClient();
            this.topic = topic;
            this.clazz = clazz;
        } catch (Exception e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> pushAsync(String key, T loadData) {
        validateProducer();
        return producer.newMessage().key(key).value(loadData).sendAsync().thenAccept(__ -> {});
    }

    @Override
    public synchronized CompletableFuture<Void> removeAsync(String key) {
        validateProducer();
        return producer.newMessage().key(key).value(null).sendAsync().thenAccept(__ -> {});
    }

    @Override
    public synchronized Optional<T> get(String key) {
        validateTableView();
        return Optional.ofNullable(tableView.get(key));
    }

    @Override
    public synchronized void forEach(BiConsumer<String, T> action) {
        validateTableView();
        tableView.forEach(action);
    }

    public synchronized Set<Map.Entry<String, T>> entrySet() {
        validateTableView();
        return tableView.entrySet();
    }

    @Override
    public synchronized int size() {
        validateTableView();
        return tableView.size();
    }

    @Override
    public synchronized void closeTableView() throws IOException {
        if (tableView != null) {
            tableView.close();
            tableView = null;
        }
    }

    @Override
    public synchronized void start() throws LoadDataStoreException {
        startProducer();
        startTableView();
    }

    @Override
    public synchronized void startTableView() throws LoadDataStoreException {
        if (tableView == null) {
            try {
                tableView = client.newTableViewBuilder(Schema.JSON(clazz)).topic(topic).create();
                tableView.forEachAndListen((k, v) ->
                        tableViewLastUpdateTimestamp = System.currentTimeMillis());
            } catch (PulsarClientException e) {
                tableView = null;
                throw new LoadDataStoreException(e);
            }
        }
    }

    @Override
    public synchronized void startProducer() throws LoadDataStoreException {
        if (producer == null) {
            try {
                producer = client.newProducer(Schema.JSON(clazz)).topic(topic).create();
            } catch (PulsarClientException e) {
                producer = null;
                throw new LoadDataStoreException(e);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        closeTableView();
    }

    @Override
    public synchronized void init() throws IOException {
        close();
        start();
    }

    private void validateProducer() {
        if (producer == null || !producer.isConnected()) {
            try {
                if (producer != null) {
                    producer.close();
                }
                producer = null;
                startProducer();
                log.info("Restarted producer on {}", topic);
            } catch (Exception e) {
                log.error("Failed to restart producer on {}", topic, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void validateTableView() {
        String restartReason = null;

        if (tableView == null) {
            restartReason = "table view is null";
        } else {
            long inactiveDuration = System.currentTimeMillis() - tableViewLastUpdateTimestamp;
            long threshold = TimeUnit.MINUTES.toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes())
                    * LOAD_DATA_REPORT_UPDATE_MAX_INTERVAL_MULTIPLIER_BEFORE_RESTART;
            if (inactiveDuration > threshold) {
                restartReason = String.format("inactiveDuration=%d secs > threshold = %d secs",
                        TimeUnit.MILLISECONDS.toSeconds(inactiveDuration),
                        TimeUnit.MILLISECONDS.toSeconds(threshold));
            }
        }

        if (StringUtils.isNotBlank(restartReason)) {
            tableViewLastUpdateTimestamp = 0;
            try {
                closeTableView();
                startTableView();
                log.info("Restarted tableview on {}, {}", topic, restartReason);
            } catch (Exception e) {
                log.error("Failed to restart tableview on {}", topic, e);
                throw new RuntimeException(e);
            }
        }
    }
}
