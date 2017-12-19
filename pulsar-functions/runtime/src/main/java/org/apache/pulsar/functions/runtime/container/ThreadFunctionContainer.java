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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.instance.JavaExecutionResult;
import org.apache.pulsar.functions.instance.JavaInstance;
import org.apache.pulsar.functions.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.spawner.ExecutionResult;

/**
 * A function container implemented using java thread.
 */
@Slf4j
class ThreadFunctionContainer implements FunctionContainer {

    class Payload {
        public String topicName;
        public String messageId;
        public byte[] msgData;
        Payload(String topicName, String messageId, byte[] msgData) {
            this.topicName = topicName;
            this.messageId = messageId;
            this.msgData = msgData;
        }
    }

    // The thread that invokes the function
    @Getter
    private final Thread fnThread;

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;

    private final JavaInstanceConfig javaInstanceConfig;
    private JavaInstance javaInstance;
    private final FunctionCacheManager fnCache;
    private LinkedBlockingQueue<Payload> queue;
    private LinkedBlockingQueue<JavaExecutionResult> resultQueue;
    private String id;

    ThreadFunctionContainer(JavaInstanceConfig instanceConfig,
                            FunctionCacheManager fnCache, ThreadGroup threadGroup) {
        this.javaInstanceConfig = instanceConfig;
        this.fnCache = fnCache;
        this.queue = new LinkedBlockingQueue<>();
        this.resultQueue = new LinkedBlockingQueue<>();
        this.id = "fn-" + instanceConfig.getFunctionConfig().getName() + "-instance-" + instanceConfig.getInstanceId();
        this.fnThread = new Thread(threadGroup,
                new Runnable() {
                    @Override
                    public void run() {
                        javaInstance = new JavaInstance(javaInstanceConfig);

                        while (true) {
                            JavaExecutionResult result;
                            try {
                                Payload payload = queue.take();
                                result = javaInstance.handleMessage(payload.messageId,
                                    payload.topicName, payload.msgData);
                                resultQueue.offer(result);
                            } catch (InterruptedException ie) {
                                log.info("Function thread {} is interrupted", ie);
                            }
                        }
                    }
                }, this.id);
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() throws Exception {

        log.info("Loading JAR files for function {}", javaInstanceConfig);

        // create the function class loader
        fnCache.registerFunctionInstance(
            javaInstanceConfig.getFunctionId(),
            javaInstanceConfig.getInstanceId(),
            javaInstanceConfig.getFunctionConfig().getJarFiles(),
            Collections.emptyList());
        log.info("Initialize function class loader for function {} at function cache manager",
            javaInstanceConfig.getFunctionConfig().getName());

        this.fnClassLoader = fnCache.getClassLoader(javaInstanceConfig.getFunctionId());
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        // make sure the function class loader is accessible thread-locally
        fnThread.setContextClassLoader(fnClassLoader);

        // javaInstance = new JavaInstance(javaInstanceConfig);

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
            javaInstanceConfig.getFunctionId(),
            javaInstanceConfig.getInstanceId());
        log.info("Unloading JAR files for function {}", javaInstanceConfig);
    }

    @Override
    public CompletableFuture<ExecutionResult> sendMessage(String topicName, String messageId, byte[] data) {
        queue.offer(new Payload(topicName, messageId, data));
        try {
            return CompletableFuture.completedFuture(resultQueue.take());
        } catch (InterruptedException e) {
            CompletableFuture<ExecutionResult> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public FunctionConfig getFunctionConfig() {
        return javaInstanceConfig.getFunctionConfig();
    }
}
