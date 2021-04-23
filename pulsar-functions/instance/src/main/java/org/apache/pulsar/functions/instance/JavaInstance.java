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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

import java.util.Map;

/**
 * This is the Java Instance. This is started by the runtimeSpawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
@Slf4j
public class JavaInstance implements AutoCloseable {

    @Getter(AccessLevel.PACKAGE)
    private final ContextImpl context;
    private Function function;
    private java.util.function.Function javaUtilFunction;

    // for Async function max out standing items
    private final InstanceConfig instanceConfig;
    private final ExecutorService executor;
    @Getter
    private final LinkedBlockingQueue<CompletableFuture<Void>> pendingAsyncRequests;

    @SuppressWarnings("rawtypes")
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
    
    /**
     * Invokes the function code against the given input data.
     * 
     * @param input - The input data provided to the Function code
     * @return An ExecutionResult object that contains the function result along with any user exceptions
     * that occurred when executing the Function code.
     */
    @SuppressWarnings("unchecked")
	private JavaExecutionResult executeFunction(Object input) {
    	JavaExecutionResult executionResult = new JavaExecutionResult();
    	
        try { 	
        	final Object result = (function != null) ? 
        		function.process(input, context) :  // For classes that implement the org.apache.pulsar.functions.api.Function interface
        		javaUtilFunction.apply(input);  // For classes that implement the java.util.Function interface
        	
            executionResult.setResult(result);  
        } catch (Exception ex) {
            executionResult.setUserException(ex);
        } 
    	
    	return executionResult;
    }
    
    /**
     * Used to handle asynchronous function requests.
     * 
     * @param future - The CompleteableFuture returned from the async function call.
     * @param executionResult 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private void handleAsync(final CompletableFuture future, JavaExecutionResult executionResult) {
    	try {
            pendingAsyncRequests.put(future);
            
            future.whenCompleteAsync((functionResult, throwable) -> {
                if (log.isDebugEnabled()) {
                  log.debug("Got result async: object: {}, throwable: {}", functionResult, throwable);
                }
                
                if (throwable != null) {
                  Exception wrappedEx = new Exception((Throwable)throwable);
                  if (isSystemException(throwable)) {
                	executionResult.setSystemException(wrappedEx);  
                  } else {
                    executionResult.setUserException(wrappedEx); 
                  }
                  future.completeExceptionally(wrappedEx);
                } else {
                  future.complete(functionResult);
                }
              
                pendingAsyncRequests.remove(future); 
                
            }, executor);
              
            executionResult.setResult(future);
            
        } catch (InterruptedException ie) {
            log.warn("Exception while put Async requests", ie);
            executionResult.setSystemException(ie);
            future.completeExceptionally(ie);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Invokes the Function code against the given input data and if the Function is async
     * then it handles the async processing of the message.
     * 
     * @param record
     * @param input
     * @return
     */
    @SuppressWarnings({ "rawtypes" })
	public JavaExecutionResult handleMessage(Record<?> record, Object input) {
        if (context != null) {
            context.setCurrentMessageContext(record);
        }

        JavaExecutionResult executionResult = executeFunction(input);

        if (executionResult.getResult() instanceof CompletableFuture) {
        	// Function is in format: Function<I, CompletableFuture<O>>
            handleAsync((CompletableFuture) executionResult.getResult(), executionResult);
        } else {
        	// The function result is contained in the result field of the executionResult object
            if (log.isDebugEnabled()) {
                log.debug("Got result: object: {}", executionResult.getResult());
            }
        }

        return executionResult;
    }

    @Override
    public void close() {
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
    
    private static boolean isSystemException(Object throwable) {
    	return (throwable instanceof InterruptedException);
    }
}
