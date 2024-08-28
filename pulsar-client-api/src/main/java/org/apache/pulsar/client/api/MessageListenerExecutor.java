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

/**
 * Interface for providing service to execute message listeners.
 */
public interface MessageListenerExecutor {

    /**
     * select a thread by message to execute the runnable!
     * <p>
     * Suggestions:
     * <p>
     * 1. The message listener task will be submitted to this executor for execution,
     * so the implementations of this interface should carefully consider execution
     * order if sequential consumption is required.
     * </p>
     * <p>
     * 2. The users should release resources(e.g. threads) of the executor after closing
     * the consumer to avoid leaks.
     * </p>
     * @param message  the message
     * @param runnable the runnable to execute, that is, the message listener task
     */
    void execute(Message<?> message, Runnable runnable);
}
