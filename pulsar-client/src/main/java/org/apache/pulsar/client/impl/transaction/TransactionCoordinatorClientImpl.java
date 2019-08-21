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
package org.apache.pulsar.client.impl.transaction;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The implementation of {@link TransactionCoordinatorClient}.
 */
@Slf4j
public class TransactionCoordinatorClientImpl implements TransactionCoordinatorClient{

    private final PulsarClientImpl client;
    private final Backoff backoff;

    private final ConcurrentHashMap<TopicName, ClientCnx> lookUpCache = new ConcurrentHashMap<>();


    public TransactionCoordinatorClientImpl(PulsarClientImpl client) {
        this.client = client;

        this.backoff = new BackoffBuilder()
            .setInitialTime(100, TimeUnit.MILLISECONDS)
            .setMax(60, TimeUnit.SECONDS)
            .setMandatoryStop(0, TimeUnit.MILLISECONDS)
            .useUserConfiguredIntervals(client.getConfiguration().getDefaultBackoffIntervalNanos(),
                                        client.getConfiguration().getMaxBackoffIntervalNanos())
            .create();
    }

    @Override
    public CompletableFuture<Void> commitTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        long requestId = client.newRequestId();
        ByteBuf commitTxn = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits, topic,
                                                          PulsarApi.TxnAction.COMMIT);
        return sendCommands(topic, commitTxn);
    }

    @Override
    public CompletableFuture<Void> abortTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        long requestId = client.newRequestId();
        ByteBuf abortTxn = Commands.newEndTxnOnPartition(requestId, txnIdLeastBits, txnIdMostBits, topic,
                                                         PulsarApi.TxnAction.ABORT);
        return sendCommands(topic, abortTxn);
    }

    @Override
    public CompletableFuture<Void> commitTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(new UnsupportedOperationException("Not Implemented Yet"));
    }

    @Override
    public CompletableFuture<Void> abortTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return FutureUtil.failedFuture(new UnsupportedOperationException("Not Implemented Yet"));
    }

    CompletableFuture<Void> sendCommands(String topic, ByteBuf commands) {
        CompletableFuture<Void> sendFuture = new CompletableFuture<>();

        getOrCreateClientCnx(topic)
            .thenCompose(clientCnx -> clientCnx.sendTxnRequestToTBWithId(commands, client.newRequestId()))
            .whenComplete((ignore, err) -> {
                if (err != null && err.getCause() instanceof PulsarClientException) {
                    sendFuture.completeExceptionally(err);
                } else if (err != null) {
                    if (lookUpCache.containsKey(TopicName.get(topic))) {
                        lookUpCache.remove(TopicName.get(topic));
                    }
                    long delayMs = backoff.next();
                    err.printStackTrace();
                    log.info("[{}] Could not get connection to broker:  {} -- Will try again in {} s", topic,
                             err.getMessage(), delayMs / 1000.0);
                    client.timer().newTimeout(timeout -> {
                        sendCommands(topic, commands);
                    }, delayMs, TimeUnit.MILLISECONDS);
                } else {
                    sendFuture.complete(null);
                }
            });

        return sendFuture;
    }

    CompletableFuture<ClientCnx> getOrCreateClientCnx(String topic) {
        TopicName topicName = TopicName.get(topic);
        if (lookUpCache.containsKey(topicName)) {
            return CompletableFuture.completedFuture(lookUpCache.get(topicName));
        }

        return client.getLookup().getBroker(topicName)
                     .thenCompose(pair -> client.getCnxPool().getConnection(pair.getLeft(), pair.getRight()))
                     .thenApply(clientCnx -> {
                         lookUpCache.put(topicName, clientCnx);
                         return clientCnx;
                     });
    }
}
