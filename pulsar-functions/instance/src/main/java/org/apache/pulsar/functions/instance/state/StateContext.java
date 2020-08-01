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
package org.apache.pulsar.functions.instance.state;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A state context per function.
 */
public interface StateContext {

    /**
     * Increment the given <i>key</i> by the given <i>amount</i>.
     *
     * @param key key to increment
     * @param amount the amount incremented
     */
    CompletableFuture<Void> incrCounter(String key, long amount) throws Exception;

    /**
     * Update the given <i>key</i> to the provide <i>value</i>.
     *
     * <p>NOTE: the put operation might or might not be applied directly to the global state until
     * the state is flushed via {@link #flush()} at the completion of function execution.
     *
     * <p>The behavior of `PUT` is non-deterministic, if two function instances attempt to update
     * same key around the same time, there is no guarantee which update will be the final result.
     * That says, if you attempt to get amount via {@link #getAmount(String)}, increment the amount
     * based on the function computation logic, and update the computed amount back. one update will
     * overwrite the other update. For this case, you are encouraged to use {@link #incr(String, long)}
     * instead.
     *
     * @param key key to update.
     * @param value value to update; if null the key is deleted
     */
    CompletableFuture<Void> put(String key, ByteBuffer value) throws Exception;

    /**
     * Deletes the <i>value</i> at the given <i>key</i>
     *
     * @param key to delete
     */
    CompletableFuture<Void> delete(String key);

    /**
     * Get the value of a given <i>key</i>.
     *
     * @param key key to retrieve
     * @return a completable future representing the retrieve result.
     */
    CompletableFuture<ByteBuffer> get(String key) throws Exception;

    /**
     * Get the amount of a given <i>key</i>.
     *
     * @param key key to retrieve
     * @return a completable future representing the retrieve result.
     */
    CompletableFuture<Long> getCounter(String key) throws Exception;

}
