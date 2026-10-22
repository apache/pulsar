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
 * A binary-protocol {@code CommandAuthChallenge} payload handed to a
 * {@link BinaryAuthChallengeHandler} (PIP-478).
 *
 * <p><b>Ownership: the array is handed off, not copied.</b> The framework transfers ownership of
 * {@code bytes} to this value on construction and the handler reads it without copying; neither
 * side may mutate the array afterwards. Because the component is an array, the record's generated
 * {@code equals}/{@code hashCode} are reference-based, not value-based; do not compare
 * {@link AuthChallenge} instances for value equality.
 *
 * @param bytes the challenge bytes sent by the broker
 */
public record AuthChallenge(byte[] bytes) {
}
