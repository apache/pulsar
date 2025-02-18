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

package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class ServiceUnitStateTableViewImpl extends ServiceUnitStateTableViewBase {

    public static final String TOPIC = TopicName.get(
            TopicDomain.persistent.value(),
            SYSTEM_NAMESPACE,
            "loadbalancer-service-unit-state").toString();
    private static final int MAX_OUTSTANDING_PUB_MESSAGES = 500;
    public static final CompressionType MSG_COMPRESSION_TYPE = CompressionType.ZSTD;
    private volatile Producer<ServiceUnitStateData> producer;
    private volatile TableView<ServiceUnitStateData> tableview;

    public void start(PulsarService pulsar,
                      BiConsumer<String, ServiceUnitStateData> tailItemListener,
                      BiConsumer<String, ServiceUnitStateData> existingItemListener) throws IOException {
        boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);

        init(pulsar);

        var schema = Schema.JSON(ServiceUnitStateData.class);

        ExtensibleLoadManagerImpl.createSystemTopic(pulsar, TOPIC);

        if (producer != null) {
            producer.close();
            if (debug) {
                log.info("Closed the channel producer.");
            }
        }

        producer = pulsar.getClient().newProducer(schema)
                .enableBatching(true)
                .compressionType(MSG_COMPRESSION_TYPE)
                .maxPendingMessages(MAX_OUTSTANDING_PUB_MESSAGES)
                .blockIfQueueFull(true)
                .topic(TOPIC)
                .create();

        if (debug) {
            log.info("Successfully started the channel producer.");
        }

        if (tableview != null) {
            tableview.close();
            if (debug) {
                log.info("Closed the channel tableview.");
            }
        }

        tableview = pulsar.getClient().newTableViewBuilder(schema)
                .topic(TOPIC)
                .loadConf(Map.of(
                        "topicCompactionStrategyClassName",
                        ServiceUnitStateDataConflictResolver.class.getName()))
                .create();
        tableview.listen(this::updateOwnedServiceUnits);
        tableview.listen(tailItemListener);
        tableview.forEach(this::updateOwnedServiceUnits);
        tableview.forEach(existingItemListener);

    }

    private boolean isValidState() {
        if (tableview == null || producer == null) {
            return false;
        }
        return true;
    }


    @Override
    public void close() throws IOException {

        if (tableview != null) {
            tableview.close();
            tableview = null;
            log.info("Successfully closed the channel tableview.");
        }

        if (producer != null) {
            producer.close();
            producer = null;
            log.info("Successfully closed the channel producer.");
        }
    }

    @Override
    public ServiceUnitStateData get(String key) {
        if (!isValidState()) {
            throw new IllegalStateException(INVALID_STATE_ERROR_MSG);
        }
        return tableview.get(key);
    }

    @Override
    public CompletableFuture<Void> put(String key, ServiceUnitStateData value) {
        if (!isValidState()) {
            return CompletableFuture.failedFuture(new IllegalStateException(INVALID_STATE_ERROR_MSG));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        producer.newMessage()
                .key(key)
                .value(value)
                .sendAsync()
                .whenComplete((messageId, e) -> {
                    if (e != null) {
                        if (e instanceof PulsarClientException.AlreadyClosedException) {
                            log.info("Skip publishing the message since the producer is closed, serviceUnit: {}, data: "
                                    + "{}", key, value);
                        } else {
                            log.error("Failed to publish the message: serviceUnit:{}, data:{}",
                                    key, value, e);
                        }
                        future.completeExceptionally(e);
                    } else {
                        future.complete(null);
                    }
                });
        return future;
    }

    @Override
    public void flush(long waitDurationInMillis) throws InterruptedException, TimeoutException, ExecutionException {
        if (!isValidState()) {
            throw new IllegalStateException(INVALID_STATE_ERROR_MSG);
        }
        final var deadline = System.currentTimeMillis() + waitDurationInMillis;
        var waitTimeMs = waitDurationInMillis;
        producer.flushAsync().get(waitTimeMs, MILLISECONDS);
        waitTimeMs = deadline - System.currentTimeMillis();
        if (waitTimeMs < 0) {
            waitTimeMs = 0;
        }
        tableview.refreshAsync().get(waitTimeMs, MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return put(key, null);
    }

    @Override
    public Set<Map.Entry<String, ServiceUnitStateData>> entrySet() {
        if (!isValidState()) {
            throw new IllegalStateException(INVALID_STATE_ERROR_MSG);
        }
        return tableview.entrySet();
    }
}
