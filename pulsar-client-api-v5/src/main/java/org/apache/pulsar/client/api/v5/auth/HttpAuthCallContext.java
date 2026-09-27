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

import java.net.URI;
import java.util.Optional;

/**
 * The per-call context for HTTP authentication (PIP-478).
 *
 * <p>Carries the request URI, a per-exchange state slot (with the same contract as on
 * {@link AuthenticationCallContext}), and — for SASL-style challenge/response — the server's challenge
 * headers from the prior {@code 401} response.
 */
public interface HttpAuthCallContext {

    /**
     * @return the URI of the request being authenticated
     */
    URI requestUri();

    /**
     * For SASL-style HTTP challenge/response: the challenge headers from the server's prior
     * {@code 401} response (for example {@code SASL-Token} / {@code State} / {@code SASL-Server-ID}).
     * Empty on the first request, before any challenge.
     *
     * @return the server's challenge headers, if a challenge has been received
     */
    Optional<HttpAuthHeaders> serverChallengeHeaders();

    /**
     * Retrieve an implementation-controlled state object previously stored with
     * {@link #setStateObject}. The slot is keyed by class; its lifetime equals one HTTP request's retry
     * sequence. See {@link AuthenticationCallContext#getStateObject} for the full contract.
     *
     * @param clazz the state object's key class
     * @param <T>   the state object type
     * @return the stored object, if present
     */
    <T> Optional<T> getStateObject(Class<T> clazz);

    /**
     * Store a state object keyed by its (or any) class; a {@code null} value removes the entry. Rounds
     * of one exchange are serialized by the framework, so slot access within an exchange needs no
     * synchronization.
     *
     * @param clazz the key class
     * @param value the state object, or {@code null} to remove the entry
     * @param <T>   the state object type
     */
    <T> void setStateObject(Class<T> clazz, T value);
}
