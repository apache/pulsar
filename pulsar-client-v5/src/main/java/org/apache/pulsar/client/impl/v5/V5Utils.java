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
package org.apache.pulsar.client.impl.v5;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Internal utilities for the V5 client implementation.
 */
final class V5Utils {

    /**
     * The V5 types of the error categories of error-model.md §2, by the v4 exceptions that report them. No v4
     * type here is a subtype of another one, so a failure matches one entry at most.
     */
    private static final List<V5Type> V5_TYPES = List.of(
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.NotFoundException.class,
                    PulsarClientException.NotFoundException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException.class,
                    PulsarClientException.NotFoundException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.AuthenticationException.class,
                    PulsarClientException.AuthenticationException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException.class,
                    PulsarClientException.GettingAuthenticationDataException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException.class,
                    PulsarClientException.UnsupportedAuthenticationException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.AuthorizationException.class,
                    PulsarClientException.AuthorizationException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.TimeoutException.class,
                    PulsarClientException.TimeoutException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException.class,
                    PulsarClientException.AlreadyClosedException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException.class,
                    PulsarClientException.ProducerBusyException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.ProducerFencedException.class,
                    PulsarClientException.ProducerBusyException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException.class,
                    PulsarClientException.ConsumerBusyException::new),
            // Overlapping Key_Shared hash ranges: a subscription constraint that keeps the consumer out.
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.ConsumerAssignException.class,
                    PulsarClientException.ConsumerBusyException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException.class,
                    PulsarClientException.IncompatibleSchemaException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException.class,
                    PulsarClientException.TopicTerminatedException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.ConnectException.class,
                    PulsarClientException.ConnectException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.NotConnectedException.class,
                    PulsarClientException.NotConnectedException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError.class,
                    PulsarClientException.ProducerQueueIsFullException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.MemoryBufferIsFullError.class,
                    PulsarClientException.MemoryBufferIsFullException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.TransactionConflictException.class,
                    PulsarClientException.TransactionConflictException::new),
            // A transaction with a failed operation, no longer open, or no longer known to its coordinator (timed
            // out) cannot be committed or aborted (client-api.md).
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.TransactionHasOperationFailedException.class,
                    PulsarClientException.TransactionConflictException::new),
            new V5Type(TransactionCoordinatorClientException.InvalidTxnStatusException.class,
                    PulsarClientException.TransactionConflictException::new),
            new V5Type(TransactionCoordinatorClientException.TransactionNotFoundException.class,
                    PulsarClientException.TransactionConflictException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.CryptoException.class,
                    PulsarClientException.CryptoException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException.class,
                    PulsarClientException.InvalidConfigurationException::new),
            new V5Type(org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL.class,
                    PulsarClientException.InvalidServiceURLException::new));

    private V5Utils() {
    }

    /**
     * Parse a topic string passed to a V5 builder and validate it for the scalable-topic
     * lookup session.
     *
     * <p>Accepts three input forms, preserving the domain so the broker can decide
     * whether the lookup resolves to a real DAG (natively scalable topic) or a synthetic
     * layout (regular topic that has not yet been migrated):
     * <ul>
     *   <li>{@code topic://tenant/ns/x} — explicitly scalable.</li>
     *   <li>{@code persistent://tenant/ns/x} — regular topic; if it has been migrated the
     *       broker promotes it to its {@code topic://} identity and returns the real DAG,
     *       otherwise it returns a synthetic layout that wraps the existing partitions.</li>
     *   <li>Short forms ({@code my-topic} or {@code tenant/ns/my-topic}) — normalised by
     *       {@link TopicName#get(String)} to {@code persistent://public/default/...} or
     *       {@code persistent://tenant/ns/...}; treated like any other persistent input.</li>
     * </ul>
     *
     * <p>Rejects {@code non-persistent://} with {@link UnsupportedOperationException}:
     * scalable topics are always backed by managed ledgers, and the V5 SDK has no path
     * to a non-persistent topic.
     */
    static TopicName parseScalableTopicInput(String topic) {
        TopicName tn = TopicName.get(topic);
        if (tn.getDomain() == TopicDomain.non_persistent) {
            throw new UnsupportedOperationException(
                    "V5 does not support non-persistent:// topics: " + topic);
        }
        return tn;
    }

    /**
     * The V5 exception to hand the application for the failure of an operation that the V5 client runs on the
     * v4 client, so that it can branch on the error category (error-model.md §2): the V5 type of its category,
     * or the plain {@link PulsarClientException} for a failure outside them, either one caused by the original
     * failure. A V5 exception is returned as is. A {@code CompletionException} or {@code ExecutionException} is
     * unwrapped first.
     */
    static PulsarClientException toV5Exception(Throwable failure) {
        Throwable cause = FutureUtil.unwrapCompletionException(failure);
        if (cause instanceof PulsarClientException v5Exception) {
            return v5Exception;
        }
        for (V5Type type : V5_TYPES) {
            if (type.v4Type().isInstance(cause)) {
                PulsarClientException typed = type.constructor().apply(cause.getMessage());
                // Unlike the plain exception, most of these types have no constructor that takes the cause.
                typed.initCause(cause);
                return typed;
            }
        }
        return new PulsarClientException(cause.getMessage(), cause);
    }

    /** A future failed with the {@link #toV5Exception V5 exception} for {@code failure}. */
    static <T> CompletableFuture<T> failWithV5Exception(Throwable failure) {
        return CompletableFuture.failedFuture(toV5Exception(failure));
    }

    /** The V5 exception type, by its message constructor, for the v4 exceptions of {@code v4Type}. */
    private record V5Type(Class<? extends Exception> v4Type, Function<String, PulsarClientException> constructor) {
    }
}
