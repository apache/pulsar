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
    private static final String SHUTDOWN_ERR_MSG = "This load store tableview has been shutdown";
    private static final long INIT_TIMEOUT_IN_SECS = 5;
    private volatile TableView<T> tableView;
    private volatile long tableViewLastUpdateTimestamp;
    private volatile long producerLastPublishTimestamp;
    private volatile Producer<T> producer;
    private final ServiceConfiguration conf;
    private final PulsarClient client;
    private final String topic;
    private final Class<T> clazz;
    private volatile boolean isShutdown;

    public TableViewLoadDataStoreImpl(PulsarService pulsar, String topic, Class<T> clazz)
            throws LoadDataStoreException {
        try {
            this.conf = pulsar.getConfiguration();
            this.client = pulsar.getClient();
            this.topic = topic;
            this.clazz = clazz;
            this.isShutdown = false;
        } catch (Exception e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> pushAsync(String key, T loadData) {
        String msg = validateProducer();
        if (StringUtils.isNotBlank(msg)) {
            return CompletableFuture.failedFuture(new IllegalStateException(msg));
        }
        return producer.newMessage().key(key).value(loadData).sendAsync()
                .thenAccept(__ -> producerLastPublishTimestamp = System.currentTimeMillis());
    }

    @Override
    public synchronized CompletableFuture<Void> removeAsync(String key) {
        String msg = validateProducer();
        if (StringUtils.isNotBlank(msg)) {
            return CompletableFuture.failedFuture(new IllegalStateException(msg));
        }
        return producer.newMessage().key(key).value(null).sendAsync()
                .thenAccept(__ -> producerLastPublishTimestamp = System.currentTimeMillis());
    }

    @Override
    public synchronized Optional<T> get(String key) {
        String msg = validateTableView();
        if (StringUtils.isNotBlank(msg)) {
            if (msg.equals(SHUTDOWN_ERR_MSG)) {
                return Optional.empty();
            } else {
                throw new IllegalStateException(msg);
            }
        }
        return Optional.ofNullable(tableView.get(key));
    }

    @Override
    public synchronized void forEach(BiConsumer<String, T> action) {
        String msg = validateTableView();
        if (StringUtils.isNotBlank(msg)) {
            throw new IllegalStateException(msg);
        }
        tableView.forEach(action);
    }

    public synchronized Set<Map.Entry<String, T>> entrySet() {
        String msg = validateTableView();
        if (StringUtils.isNotBlank(msg)) {
            throw new IllegalStateException(msg);
        }
        return tableView.entrySet();
    }

    @Override
    public synchronized int size() {
        String msg = validateTableView();
        if (StringUtils.isNotBlank(msg)) {
            throw new IllegalStateException(msg);
        }
        return tableView.size();
    }

    private void validateState() {
        if (isShutdown) {
            throw new IllegalStateException(SHUTDOWN_ERR_MSG);
        }
    }


    @Override
    public synchronized void init() throws IOException {
        validateState();
        close();
        start();
    }

    @Override
    public synchronized void closeTableView() throws IOException {
        validateState();
        if (tableView != null) {
            tableView.close();
            tableView = null;
        }
    }

    @Override
    public synchronized void start() throws LoadDataStoreException {
        validateState();
        startProducer();
        startTableView();
    }

    private synchronized void closeProducer() throws IOException {
        validateState();
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
    @Override
    public synchronized void startTableView() throws LoadDataStoreException {
        validateState();
        if (tableView == null) {
            try {
                tableView = client.newTableViewBuilder(Schema.JSON(clazz)).topic(topic).createAsync()
                        .get(INIT_TIMEOUT_IN_SECS, TimeUnit.SECONDS);
                tableViewLastUpdateTimestamp = System.currentTimeMillis();
                tableView.forEachAndListen((k, v) ->
                        tableViewLastUpdateTimestamp = System.currentTimeMillis());
            } catch (Exception e) {
                tableView = null;
                throw new LoadDataStoreException(e);
            }
        }
    }
    @Override
    public synchronized void startProducer() throws LoadDataStoreException {
        validateState();
        if (producer == null) {
            try {
                producer = client.newProducer(Schema.JSON(clazz)).topic(topic).createAsync()
                        .get(INIT_TIMEOUT_IN_SECS, TimeUnit.SECONDS);
                producerLastPublishTimestamp = System.currentTimeMillis();
            } catch (Exception e) {
                producer = null;
                throw new LoadDataStoreException(e);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isShutdown) {
            return;
        }
        closeProducer();
        closeTableView();
    }

    @Override
    public synchronized void shutdown() throws IOException {
        close();
        isShutdown = true;
    }

    private String validateProducer() {
        if (isShutdown) {
            return SHUTDOWN_ERR_MSG;
        }
        String restartReason = getRestartReason(producer, producerLastPublishTimestamp);
        if (StringUtils.isNotBlank(restartReason)) {
            try {
                closeProducer();
                startProducer();
                log.info("Restarted producer on {}, {}", topic, restartReason);
            } catch (Exception e) {
                String msg = "Failed to restart producer on " + topic + ", restart reason: " + restartReason;
                log.error(msg, e);
                return msg;
            }
        }
        return null;
    }

    private String validateTableView() {
        if (isShutdown) {
            return SHUTDOWN_ERR_MSG;
        }
        String restartReason = getRestartReason(tableView, tableViewLastUpdateTimestamp);
        if (StringUtils.isNotBlank(restartReason)) {
            try {
                closeTableView();
                startTableView();
                log.info("Restarted tableview on {}, {}", topic, restartReason);
            } catch (Exception e) {
                String msg = "Failed to tableview on " + topic + ", restart reason: " + restartReason;
                log.error(msg, e);
                return msg;
            }
        }
        return null;
    }

    private String getRestartReason(Object obj, long lastUpdateTimestamp) {

        String restartReason = null;

        if (obj == null) {
            restartReason = "object is null";
        } else {
            long inactiveDuration = System.currentTimeMillis() - lastUpdateTimestamp;
            long threshold = TimeUnit.MINUTES.toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes())
                    * LOAD_DATA_REPORT_UPDATE_MAX_INTERVAL_MULTIPLIER_BEFORE_RESTART;
            if (inactiveDuration > threshold) {
                restartReason = String.format("inactiveDuration=%d secs > threshold = %d secs",
                        TimeUnit.MILLISECONDS.toSeconds(inactiveDuration),
                        TimeUnit.MILLISECONDS.toSeconds(threshold));
            }
        }
        return restartReason;
    }
}
