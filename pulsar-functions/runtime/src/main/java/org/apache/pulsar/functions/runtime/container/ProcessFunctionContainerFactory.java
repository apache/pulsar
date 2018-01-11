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
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManagerImpl;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
public class ProcessFunctionContainerFactory implements FunctionContainerFactory {

    private int maxBufferedTuples;
    private String pulsarServiceUrl;
    private String javaInstanceJarFile;
    private String logDirectory;

    @VisibleForTesting
    public ProcessFunctionContainerFactory(int maxBufferedTuples,
                                           String pulsarServiceUrl,
                                           String javaInstanceJarFile,
                                           String logDirectory) {

        this.maxBufferedTuples = maxBufferedTuples;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.javaInstanceJarFile = javaInstanceJarFile;
        this.logDirectory = logDirectory;
    }

    @Override
    public ProcessFunctionContainer createContainer(JavaInstanceConfig instanceConfig, String jarFile) {
        return new ProcessFunctionContainer(
            instanceConfig,
            maxBufferedTuples,
            javaInstanceJarFile,
            logDirectory,
            jarFile,
            pulsarServiceUrl);
    }

    @Override
    public void close() {
    }
}
