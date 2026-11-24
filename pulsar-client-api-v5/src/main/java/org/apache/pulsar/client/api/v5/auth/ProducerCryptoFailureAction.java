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
 * Action a producer takes when message encryption fails (e.g. the
 * {@link PublicKeyProvider} cannot be reached or returns no key).
 */
public enum ProducerCryptoFailureAction {

    /**
     * Fail the {@code send} call. The send future completes exceptionally and
     * the application sees the error.
     */
    FAIL,

    /**
     * Send the message unencrypted instead of failing. Useful when encryption
     * is opportunistic — for example, during a key-rollout migration.
     */
    SEND_UNENCRYPTED
}
