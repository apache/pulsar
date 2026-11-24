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
package org.apache.pulsar.functions.instance.v5;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder;

/**
 * Presents a V5 {@link AsyncMessageBuilder} through the v4 {@link TypedMessageBuilder} interface.
 *
 * <p>The Functions runtime builds output messages through the v4 interface; this adapter lets the same code
 * publish with the V5 client. Features the V5 client does not have ({@link #keyBytes(byte[])} and
 * {@link #orderingKey(byte[])}) throw {@link UnsupportedOperationException}.
 */
public class V5TypedMessageBuilder<T> implements TypedMessageBuilder<T> {

    private static final long serialVersionUID = 1L;
    // the same marker the v4 client uses for TypedMessageBuilder#disableReplication
    private static final List<String> LOCAL_CLUSTER_ONLY = Collections.singletonList("__local__");

    private final transient AsyncMessageBuilder<T> builder;

    public V5TypedMessageBuilder(AsyncMessageBuilder<T> builder) {
        this.builder = builder;
    }

    @Override
    public MessageId send() throws PulsarClientException {
        try {
            return sendAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<MessageId> sendAsync() {
        return builder.send().thenApply(V5MessageIdAdapter::new);
    }

    @Override
    public TypedMessageBuilder<T> key(String key) {
        builder.key(key);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> keyBytes(byte[] key) {
        throw new UnsupportedOperationException("The V5 client does not support byte[] message keys");
    }

    @Override
    public TypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
        throw new UnsupportedOperationException("The V5 client does not support ordering keys");
    }

    @Override
    public TypedMessageBuilder<T> value(T value) {
        builder.value(value);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> property(String name, String value) {
        builder.property(name, value);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> properties(Map<String, String> properties) {
        builder.properties(properties);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> eventTime(long timestamp) {
        builder.eventTime(Instant.ofEpochMilli(timestamp));
        return this;
    }

    @Override
    public TypedMessageBuilder<T> sequenceId(long sequenceId) {
        builder.sequenceId(sequenceId);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> replicationClusters(List<String> clusters) {
        builder.replicationClusters(clusters);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> disableReplication() {
        builder.replicationClusters(LOCAL_CLUSTER_ONLY);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> deliverAt(long timestamp) {
        builder.deliverAt(Instant.ofEpochMilli(timestamp));
        return this;
    }

    @Override
    public TypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        builder.deliverAfter(Duration.ofNanos(unit.toNanos(delay)));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypedMessageBuilder<T> loadConf(Map<String, Object> config) {
        config.forEach((key, value) -> {
            switch (key) {
                case CONF_KEY -> key(checkType(value, String.class));
                case CONF_PROPERTIES -> properties(checkType(value, Map.class));
                case CONF_EVENT_TIME -> eventTime(checkType(value, Long.class));
                case CONF_SEQUENCE_ID -> sequenceId(checkType(value, Long.class));
                case CONF_REPLICATION_CLUSTERS -> replicationClusters(checkType(value, List.class));
                case CONF_DISABLE_REPLICATION -> {
                    if (checkType(value, Boolean.class)) {
                        disableReplication();
                    }
                }
                case CONF_DELIVERY_AFTER_SECONDS -> deliverAfter(checkType(value, Long.class), TimeUnit.SECONDS);
                case CONF_DELIVERY_AT -> deliverAt(checkType(value, Long.class));
                default -> throw new RuntimeException("Invalid message config key '" + key + "'");
            }
        });
        return this;
    }

    private static <V> V checkType(Object value, Class<V> clazz) {
        if (!clazz.isInstance(value)) {
            throw new RuntimeException("Invalid type " + value.getClass() + " for value " + value
                    + ", expected " + clazz);
        }
        return clazz.cast(value);
    }
}
