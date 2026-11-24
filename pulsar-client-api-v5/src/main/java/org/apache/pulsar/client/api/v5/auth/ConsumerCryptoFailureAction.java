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
 * Action a consumer takes when message decryption fails (e.g. the
 * {@link PrivateKeyProvider} cannot be reached, returns no key, or the
 * ciphertext is malformed).
 */
public enum ConsumerCryptoFailureAction {

    /**
     * Fail the {@code receive} call. The application sees the decryption error
     * and the message stays unacknowledged so it will be redelivered.
     */
    FAIL,

    /**
     * Silently acknowledge and skip the message. Useful when the consumer
     * legitimately cannot read some encrypted streams (e.g. a side channel)
     * but should keep moving forward through the rest.
     */
    DISCARD,

    /**
     * Deliver the message to the application as-is, with the still-encrypted
     * payload. The application can then handle decryption out-of-band.
     */
    CONSUME
}
