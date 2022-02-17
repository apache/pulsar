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
package org.apache.pulsar.functions.instance;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

    public JavaInstance(ContextImpl contextImpl, Object userClassObject, InstanceConfig instanceConfig) {

        this.context = contextImpl;
        this.instanceConfig = instanceConfig;
        this.executor = Executors.newSingleThreadExecutor();
        this.pendingAsyncRequests = new LinkedBlockingQueue<>(this.instanceConfig.getMaxPendingAsyncRequests());

        // create the functions
        if (userClassObject instanceof Function) {
            this.function = (Function) userClassObject;
        } else {
            this.javaUtilFunction = (java.util.function.Function) userClassObject;
        }
    }

    @VisibleForTesting
    public JavaExecutionResult handleMessage(Record<?> record, Object input) {
        return handleMessage(record, input, (rec, result) -> {}, cause -> {});
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
            // Function is in format: Function<I, CompletableFuture<O>>
            AsyncFuncRequest request = new AsyncFuncRequest(
                record, (CompletableFuture) output
            );
            try {
                pendingAsyncRequests.put(request);
                ((CompletableFuture) output).whenCompleteAsync((res, cause) -> {
                    try {
                        processAsyncResults(asyncResultConsumer);
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

    private void processAsyncResults(JavaInstanceRunnable.AsyncResultConsumer resultConsumer) throws Exception {
        AsyncFuncRequest asyncResult = pendingAsyncRequests.peek();
        while (asyncResult != null && asyncResult.getProcessResult().isDone()) {
            pendingAsyncRequests.remove(asyncResult);
            JavaExecutionResult execResult = new JavaExecutionResult();

            try {
                Object result = asyncResult.getProcessResult().get();
                execResult.setResult(result);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof Exception) {
                    execResult.setUserException((Exception) e.getCause());
                } else {
                    execResult.setUserException(new Exception(e.getCause()));
                }
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
