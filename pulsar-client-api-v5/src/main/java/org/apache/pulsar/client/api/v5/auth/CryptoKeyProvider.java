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
 * Convenience interface for implementations that serve <em>both</em> public keys (for
 * producer-side encryption) and private keys (for consumer-side decryption) — for
 * example, a single PEM-file-backed key store used by both sides of an in-process
 * round trip.
 *
 * <p>Producer-only or consumer-only implementations should implement
 * {@link PublicKeyProvider} or {@link PrivateKeyProvider} directly instead — that
 * makes the role explicit and avoids stub methods that throw.
 */
public interface CryptoKeyProvider extends PublicKeyProvider, PrivateKeyProvider {
}
