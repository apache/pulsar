/**
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
package org.apache.pulsar.functions.api.state;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.functions.api.StateStore;

/**
 * A key-value state store that stores values in {@link ByteBuffer}.
 */
public interface ByteBufferStateStore extends StateStore {

    /**
     * Update the state value for the key.
     *
     * @param key   name of the key
     * @param value state value of the key
     */
    void put(String key, ByteBuffer value);

    /**
     * Update the state value for the key, but don't wait for the operation to be completed
     *
     * @param key   name of the key
     * @param value state value of the key
     */
    CompletableFuture<Void> putAsync(String key, ByteBuffer value);

    /**
     * Delete the state value for the key.
     *
     * @param key   name of the key
     */
    void delete(String key);

    /**
     * Delete the state value for the key, but don't wait for the operation to be completed
     *
     * @param key   name of the key
     */
    CompletableFuture<Void> deleteAsync(String key);

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer get(String key);

    /**
     * Retrieve the state value for the key, but don't wait for the operation to be completed
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    CompletableFuture<ByteBuffer> getAsync(String key);

}
