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

package org.apache.pulsar.functions.runtime.container;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManagerImpl;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
public class ThreadFunctionContainerFactory implements FunctionContainerFactory {

    private final ThreadGroup threadGroup;
    private final FunctionCacheManager fnCache;
    private final PulsarClient pulsarClient;
    private int maxBufferedTuples;
    private volatile boolean closed;

    public ThreadFunctionContainerFactory(int maxBufferedTuples,
                                          String pulsarServiceUrl,
                                          ClientConfiguration conf)
            throws Exception {
        this(maxBufferedTuples, pulsarServiceUrl != null ? PulsarClient.create(pulsarServiceUrl, conf) : null);
    }

    @VisibleForTesting
    ThreadFunctionContainerFactory(int maxBufferedTuples, PulsarClient pulsarClient) {
        this.fnCache = new FunctionCacheManagerImpl();
        this.threadGroup = new ThreadGroup(
            "Pulsar Function Container Threads");
        this.maxBufferedTuples = maxBufferedTuples;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public ThreadFunctionContainer createContainer(JavaInstanceConfig instanceConfig, String jarFile) {
        return new ThreadFunctionContainer(
            instanceConfig,
            maxBufferedTuples,
            fnCache,
            threadGroup,
            jarFile,
            pulsarClient);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        threadGroup.interrupt();
        fnCache.close();
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close pulsar client when closing function container factory", e);
        }
    }
}
