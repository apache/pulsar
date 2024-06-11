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
package org.apache.pulsar.client.impl;

import java.util.concurrent.ExecutorService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;

abstract class AbstractMessageListenerExecutor implements MessageListenerExecutor {
    @Override
    public void execute(Message<?> message, Runnable runnable) {
        getExecutor(message).execute(runnable);
    }

    /**
     * Get the executor for the message.
     * @param message the message
     * @return the executor
     */
    protected abstract ExecutorService getExecutor(Message<?> message);

    /**
     * Shutdown the executor now and stop accepting new tasks.
     */
    public abstract void shutdownNow();

    /**
     * Check if the executor has been shutdown.
     * @return true if the executor has been shutdown
     */
    public abstract boolean isShutdown();

}
