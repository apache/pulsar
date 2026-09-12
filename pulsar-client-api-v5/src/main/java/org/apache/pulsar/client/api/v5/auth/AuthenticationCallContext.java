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
package org.apache.pulsar.client.api.v5.auth;

import java.util.Optional;

/**
 * The per-call context for binary-protocol authentication (PIP-478).
 *
 * <p>Carries the broker host and exposes a per-exchange state slot for retaining
 * conversation state across challenge-response rounds. Most implementations produce the same credential
 * for every call and never inspect the broker host.
 */
public interface AuthenticationCallContext {

    /**
     * @return the broker host this connection is being established to
     */
    String brokerHost();

    /**
     * Retrieve an implementation-controlled state object previously stored with
     * {@link #setStateObject}. The slot is keyed by class so the pieces of one plugin — which, under
     * the capability-factory model, may be separate internal classes participating in the same exchange
     * (e.g. the initial-data provider and the challenge handler on one binary connect) — can each keep
     * state without collision; impls typically store one object of their own type (e.g. their SASL
     * conversation state).
     *
     * <p>The slot's lifetime equals one authentication <em>exchange</em>: for the binary protocol, one
     * connection attempt and all of its ordinary {@code CommandAuthChallenge} / {@code CommandAuthResponse}
     * challenge rounds. A broker {@code REFRESH_AUTH_DATA} sentinel does not continue this exchange —
     * {@code ClientCnx} begins a <em>new</em> exchange with a fresh context and a fresh slot — so
     * conversation state does not survive a refresh. Concurrent authentications to different brokers get
     * their own context with their own slots, so multiple in-flight handshakes don't collide.
     *
     * @param clazz the state object's key class
     * @param <T>   the state object type
     * @return the stored object, if present
     */
    <T> Optional<T> getStateObject(Class<T> clazz);

    /**
     * Store a state object keyed by its (or any) class; a {@code null} value removes the entry.
     * Implementations should key with a private class of their own (e.g. their conversation record) to
     * avoid collisions. Rounds of one exchange are serialized by the framework, so slot access within an
     * exchange needs no synchronization.
     *
     * @param clazz the key class
     * @param value the state object, or {@code null} to remove the entry
     * @param <T>   the state object type
     */
    <T> void setStateObject(Class<T> clazz, T value);
}
