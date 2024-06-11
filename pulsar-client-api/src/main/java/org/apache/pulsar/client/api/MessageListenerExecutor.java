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
package org.apache.pulsar.client.api;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * Interface for providing an executor service to execute message listeners.
 */
public interface MessageListenerExecutor extends Serializable {

    ExecutorService getExecutor();

    /**
     * Execute the runnable with the given shard key.
     * The runnable will be executed by same thread of the same shard key.
     *
     * @param shardKey the shard key
     * @param runnable the runnable to execute
     */
    void execute(byte[] shardKey, Runnable runnable);

    /**
     * Shutdown the provider and all thread resources of it.
     */
    void shutdownNow();

    /**
     * Check if the provider is shutdown.
     */
    boolean isShutdown();
}
