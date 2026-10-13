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
 * The binary-protocol credential: the {@code auth_data} bytes for {@code CommandConnect} (PIP-478).
 *
 * <p>The auth-method name is deliberately NOT part of this value — it comes from exactly one place,
 * {@link BinaryAuthDataProvider#authMethodName()}, so a plugin cannot contradict itself (an earlier
 * draft carried the name on both the provider and the data value, leaving "which one wins?" to the
 * javadoc). Kept as a record rather than a bare {@code byte[]} so fields can be added later without an
 * API break.
 *
 * <p><b>Ownership: the array is handed off, not copied.</b> The producer transfers ownership of
 * {@code bytes} to this value on construction, and the framework reads it without copying; neither
 * side may mutate the array after construction. (Copying is deliberately avoided — this is a
 * single-producer-to-single-consumer transfer, matching the v4 {@code AuthData} contract.) Because the
 * component is an array, the record's generated {@code equals}/{@code hashCode} are reference-based on
 * the bytes, not value-based; do not compare {@link BinaryAuthData} instances for value equality.
 *
 * @param bytes the credential bytes sent in {@code CommandConnect.auth_data}
 */
public record BinaryAuthData(byte[] bytes) {
}
