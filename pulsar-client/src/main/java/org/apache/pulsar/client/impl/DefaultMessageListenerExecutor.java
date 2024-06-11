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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.util.ExecutorProvider;

/**
 * Default implementation of {@link AbstractMessageListenerExecutor}.
 * This implementation will select thread by topic name to execute the runnable,
 * So that message with same topic name will be executed by same thread.
 *
 * <p>
 *   The caller should shut down the executor when it is no longer needed.
 * </p>
 */
public class DefaultMessageListenerExecutor extends AbstractMessageListenerExecutor {
    private final Map<String, ExecutorService> executorMap = new ConcurrentHashMap<>();
    private final ExecutorProvider executorProvider;

    public DefaultMessageListenerExecutor(int numListenerThreads, String threadPoolNamePrefix) {
        checkArgument(numListenerThreads > 0, "Number of listener threads must be greater than 0");
        checkArgument(StringUtils.isNotBlank(threadPoolNamePrefix), "Thread pool name prefix must be provided");
        this.executorProvider = new ExecutorProvider(numListenerThreads, threadPoolNamePrefix);
    }

    DefaultMessageListenerExecutor(ExecutorProvider executorProvider) {
        checkNotNull(executorProvider, "ExecutorProvider can't be null");
        this.executorProvider = executorProvider;
    }

    @Override
    protected ExecutorService getExecutor(Message<?> message) {
        ExecutorService executor = executorMap.get(message.getTopicName());
        if (executor != null) {
            return executor;
        } else {
            return executorMap.computeIfAbsent(message.getTopicName(), k -> executorProvider.getExecutor());
        }
    }

    @Override
    public void shutdownNow() {
        executorProvider.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorProvider.isShutdown();
    }
}
