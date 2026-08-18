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

import java.util.concurrent.CompletableFuture;

/**
 * Capability: multi-round challenge/response (SASL-style and custom protocols) for the Pulsar binary
 * protocol (PIP-478).
 *
 * <p>The broker's {@code REFRESH_AUTH_DATA} refresh sentinel is <em>never</em> delivered here (binary
 * routing rule 2): a refresh re-produces the current credential through
 * {@link BinaryAuthDataProvider} on a fresh exchange, so this handler only ever sees a genuine challenge
 * payload — a real round of the challenge conversation.
 */
public interface BinaryAuthChallengeHandler {

    /**
     * Respond to a binary-protocol {@code CommandAuthChallenge}.
     *
     * <p>The framework places the returned bytes in {@code CommandAuthResponse}. Completion is decided
     * by the broker (it replies with {@code CommandConnected} when satisfied), so the handler does not
     * signal it; cross-round conversation state is kept in the context's state slot. Never throws
     * synchronously: all failures are reported by completing the returned future exceptionally, never by
     * throwing on the calling thread.
     *
     * @param ctx           the per-call context (holds cross-round state)
     * @param authChallenge the challenge from the broker
     * @return a future of the response for the next {@code CommandAuthResponse}
     */
    CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext ctx,
                                                                 AuthChallenge authChallenge);
}
