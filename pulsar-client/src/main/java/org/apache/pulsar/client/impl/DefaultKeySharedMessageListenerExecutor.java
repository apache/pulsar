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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.util.ExecutorProvider;

/**
 * Default implementation of {@link AbstractMessageListenerExecutor} for
 * {@link org.apache.pulsar.client.api.SubscriptionType#Key_Shared}.
 * This implementation will select thread by order key to execute the runnable,
 * So that message with same order key will be executed by same thread.
 *
 * <p>
 *   The caller should shut down the executor when it is no longer needed.
 * </p>
 */
public class DefaultKeySharedMessageListenerExecutor extends AbstractMessageListenerExecutor{
    private static final byte[] NONE_KEY = "NONE_KEY".getBytes(StandardCharsets.UTF_8);
    private final ExecutorProvider executorProvider;

    public DefaultKeySharedMessageListenerExecutor(int numListenerThreads, String threadPoolNamePrefix) {
        checkArgument(numListenerThreads > 0, "Number of listener threads must be greater than 0");
        checkArgument(StringUtils.isNotBlank(threadPoolNamePrefix), "Thread pool name prefix must be provided");
        this.executorProvider = new ExecutorProvider(numListenerThreads, threadPoolNamePrefix);
    }

    DefaultKeySharedMessageListenerExecutor(ExecutorProvider executorProvider) {
        checkNotNull(executorProvider, "ExecutorProvider can't be null");
        this.executorProvider = executorProvider;
    }

    @Override
    protected ExecutorService getExecutor(Message<?> message) {
        return executorProvider.getExecutor(peekMessageKey(message));
    }

    @Override
    public void shutdownNow() {
        executorProvider.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorProvider.isShutdown();
    }

    private byte[] peekMessageKey(Message<?> msg) {
        byte[] key = NONE_KEY;
        if (msg.hasKey()) {
            key = msg.getKeyBytes();
        }
        if (msg.hasOrderingKey()) {
            key = msg.getOrderingKey();
        }
        return key;
    }
}
