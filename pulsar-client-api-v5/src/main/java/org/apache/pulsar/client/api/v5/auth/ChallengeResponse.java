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

/**
 * The reply from {@link BinaryAuthChallengeHandler#respondToChallengeAsync} — the bytes for the next
 * {@code CommandAuthResponse} (PIP-478).
 *
 * <p>There is no completion flag: the broker decides when the handshake is finished (it sends
 * {@code CommandConnected}); the implementation tracks any cross-round state of its own in the call
 * context's state slot. Kept as a record rather than a bare {@code byte[]} so fields can be added later
 * without an API break.
 *
 * <p><b>Ownership: the array is handed off, not copied.</b> The handler transfers ownership of
 * {@code bytes} to this value on construction and the framework reads it without copying;
 * neither side may mutate the array afterwards. Because the component is an array, the record's
 * generated {@code equals}/{@code hashCode} are reference-based, not value-based; do not compare
 * {@link ChallengeResponse} instances for value equality.
 *
 * @param bytes the bytes to send in the next {@code CommandAuthResponse}
 */
public record ChallengeResponse(byte[] bytes) {
}
