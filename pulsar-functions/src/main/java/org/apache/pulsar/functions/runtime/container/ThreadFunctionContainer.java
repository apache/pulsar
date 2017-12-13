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

import java.util.Collections;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;

/**
 * A function container implemented using java thread.
 */
@Slf4j
class ThreadFunctionContainer implements FunctionContainer {

    private final JavaInstanceConfig instanceConfig;
    private final FunctionCacheManager fnCache;

    // The thread that invokes the function
    @Getter
    private final Thread fnThread;

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;

    ThreadFunctionContainer(JavaInstanceConfig instanceConfig,
                            Runnable instanceRunnable,
                            FunctionCacheManager fnCache,
                            ThreadGroup containerThreadGroup) {
        this.instanceConfig = instanceConfig;
        this.fnCache = fnCache;
        this.fnThread = new Thread(
            containerThreadGroup,
            instanceRunnable,
            "fn-" + instanceConfig.getFunctionName() + "-instance-" + instanceConfig.getInstanceId());
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() throws Exception {

        log.info("Loading JAR files for function {}", instanceConfig);

        // create the function class loader
        fnCache.registerFunctionInstance(
            instanceConfig.getFunctionId(),
            instanceConfig.getInstanceId(),
            instanceConfig.getFunctionConfig().getJarFiles(),
            Collections.emptyList());
        log.info("Initialize function class loader for function {} at function cache manager",
            instanceConfig.getFunctionName());

        this.fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        // make sure the function class loader is accessible thread-locally
        fnThread.setContextClassLoader(fnClassLoader);

        // start the function thread
        fnThread.start();
    }

    @Override
    public void join() throws InterruptedException {
        fnThread.join();
    }

    @Override
    public void stop() {
        // interrupt the function thread
        fnThread.interrupt();
        try {
            fnThread.join();
        } catch (InterruptedException e) {
            // ignore this
        }
        // once the thread quits, clean up the instance
        fnCache.unregisterFunctionInstance(
            instanceConfig.getFunctionId(),
            instanceConfig.getInstanceId());
        log.info("Unloading JAR files for function {}", instanceConfig);
    }

}
