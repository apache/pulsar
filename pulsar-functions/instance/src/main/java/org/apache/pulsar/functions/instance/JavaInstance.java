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
package org.apache.pulsar.functions.instance;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * This is the Java Instance. This is started by the runtimeSpawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
@Slf4j
public class JavaInstance implements AutoCloseable {

    @Data
    public static class AsyncFuncRequest {
        private final Record record;
        private final CompletableFuture processResult;
        private final JavaExecutionResult result;
    }

    @Getter(AccessLevel.PACKAGE)
    private final ContextImpl context;
    private Function function;
    private java.util.function.Function javaUtilFunction;

    // for Async function max out standing items
    private final InstanceConfig instanceConfig;
    private final ExecutorService executor;
    @Getter
    private final LinkedBlockingQueue<AsyncFuncRequest> pendingAsyncRequests;
    @Getter
    private final Semaphore asyncRequestsConcurrencyLimiter;
    private final boolean asyncPreserveInputOrderForOutputMessages;

    public JavaInstance(ContextImpl contextImpl, Object userClassObject, InstanceConfig instanceConfig) {

        this.context = contextImpl;
        this.instanceConfig = instanceConfig;
        this.executor = Executors.newSingleThreadExecutor();

        asyncPreserveInputOrderForOutputMessages =
                resolveAsyncPreserveInputOrderForOutputMessages(instanceConfig);

        if (asyncPreserveInputOrderForOutputMessages) {
            this.pendingAsyncRequests = new LinkedBlockingQueue<>(this.instanceConfig.getMaxPendingAsyncRequests());
            this.asyncRequestsConcurrencyLimiter = null;
        } else {
            this.pendingAsyncRequests = null;
            this.asyncRequestsConcurrencyLimiter = new Semaphore(this.instanceConfig.getMaxPendingAsyncRequests());
        }

        // create the functions
        if (userClassObject instanceof Function) {
            this.function = (Function) userClassObject;
        } else {
            this.javaUtilFunction = (java.util.function.Function) userClassObject;
        }
    }

    // resolve whether to preserve input order for output messages for async functions
    private boolean resolveAsyncPreserveInputOrderForOutputMessages(InstanceConfig instanceConfig) {
        // no need to preserve input order for output messages if the function returns Void type
        boolean voidReturnType = instanceConfig.getFunctionDetails() != null
                && instanceConfig.getFunctionDetails().getSink() != null
                && Void.class.getName().equals(instanceConfig.getFunctionDetails().getSink().getTypeClassName());
        if (voidReturnType) {
            return false;
        }

        // preserve input order for output messages
        return true;
    }

    @VisibleForTesting
    public JavaExecutionResult handleMessage(Record<?> record, Object input) {
        return handleMessage(record, input, (rec, result) -> {
        }, cause -> {
        });
    }

    public JavaExecutionResult handleMessage(Record<?> record, Object input,
                                             JavaInstanceRunnable.AsyncResultConsumer asyncResultConsumer,
                                             Consumer<Throwable> asyncFailureHandler) {
        if (context != null) {
            context.setCurrentMessageContext(record);
        }

        JavaExecutionResult executionResult = new JavaExecutionResult();

        final Object output;

        try {
            if (function != null) {
                output = function.process(input, context);
            } else {
                output = javaUtilFunction.apply(input);
            }
        } catch (Exception ex) {
            executionResult.setUserException(ex);
            return executionResult;
        }

        if (output instanceof CompletableFuture) {
            try {
                if (asyncPreserveInputOrderForOutputMessages) {
                    // Function is in format: Function<I, CompletableFuture<O>>
                    AsyncFuncRequest request = new AsyncFuncRequest(
                            record, (CompletableFuture) output, executionResult
                    );
                    pendingAsyncRequests.put(request);
                } else {
                    asyncRequestsConcurrencyLimiter.acquire();
                }
                ((CompletableFuture<Object>) output).whenCompleteAsync((Object res, Throwable cause) -> {
                    try {
                        if (asyncPreserveInputOrderForOutputMessages) {
                            processAsyncResultsInInputOrder(asyncResultConsumer);
                        } else {
                            try {
                                if (cause != null) {
                                    executionResult.setUserException(FutureUtil.unwrapCompletionException(cause));
                                } else {
                                    executionResult.setResult(res);
                                }
                                asyncResultConsumer.accept(record, executionResult);
                            } finally {
                                asyncRequestsConcurrencyLimiter.release();
                            }
                        }
                    } catch (Throwable innerException) {
                        // the thread used for processing async results failed
                        asyncFailureHandler.accept(innerException);
                    }
                }, executor);
                return null;
            } catch (InterruptedException ie) {
                log.warn("Exception while put Async requests", ie);
                executionResult.setUserException(ie);
                return executionResult;
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Got result: object: {}", output);
            }
            executionResult.setResult(output);
            return executionResult;
        }
    }

    // processes the async results in the input order so that the order of the result messages in the output topic
    // are in the same order as the input
    private void processAsyncResultsInInputOrder(JavaInstanceRunnable.AsyncResultConsumer resultConsumer)
            throws Exception {
        AsyncFuncRequest asyncResult = pendingAsyncRequests.peek();
        while (asyncResult != null && asyncResult.getProcessResult().isDone()) {
            pendingAsyncRequests.remove(asyncResult);

            JavaExecutionResult execResult = asyncResult.getResult();
            try {
                Object result = asyncResult.getProcessResult().get();
                execResult.setResult(result);
            } catch (ExecutionException e) {
                execResult.setUserException(FutureUtil.unwrapCompletionException(e));
            }

            resultConsumer.accept(asyncResult.getRecord(), execResult);

            // peek the next result
            asyncResult = pendingAsyncRequests.peek();
        }
    }

    public void initialize() throws Exception {
        if (function != null) {
            function.initialize(context);
        }
    }

    @Override
    public void close() {
        if (function != null) {
            try {
                function.close();
            } catch (Exception e) {
                log.error("function closeResource occurred exception", e);
            }
        }

        context.close();
        executor.shutdown();
    }

    public Map<String, Double> getAndResetMetrics() {
        return context.getAndResetMetrics();
    }

    public void resetMetrics() {
        context.resetMetrics();
    }

    public Map<String, Double> getMetrics() {
        return context.getMetrics();
    }
}
