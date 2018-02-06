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
import org.apache.pulsar.functions.proto.Function;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
public class ProcessFunctionContainerFactory implements FunctionContainerFactory {

    private int maxBufferedTuples;
    private String pulsarServiceUrl;
    private String javaInstanceJarFile;
    private String pythonInstanceFile;
    private String logDirectory;

    @VisibleForTesting
    public ProcessFunctionContainerFactory(int maxBufferedTuples,
                                           String pulsarServiceUrl,
                                           String javaInstanceJarFile,
                                           String pythonInstanceFile,
                                           String logDirectory) {

        this.maxBufferedTuples = maxBufferedTuples;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.javaInstanceJarFile = javaInstanceJarFile;
        this.pythonInstanceFile = pythonInstanceFile;
        this.logDirectory = logDirectory;
    }

    @Override
    public ProcessFunctionContainer createContainer(InstanceConfig instanceConfig, String codeFile) {
        String instanceFile;
        switch (instanceConfig.getFunctionConfig().getRuntime()) {
            case JAVA:
                instanceFile = javaInstanceJarFile;
                break;
            case PYTHON:
                instanceFile = pythonInstanceFile;
                break;
            default:
                throw new RuntimeException("Unsupported Runtime " + instanceConfig.getFunctionConfig().getRuntime());
        }
        return new ProcessFunctionContainer(
            instanceConfig,
            maxBufferedTuples,
            instanceFile,
            logDirectory,
            codeFile,
            pulsarServiceUrl);
    }

    @Override
    public void close() {
    }
}
