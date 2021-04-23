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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

@Slf4j
public class JavaInstanceTest {

    /**
     * Verify that be able to run lambda functions.
     * @throws Exception
     */
    @Test
    public void testLambda() throws Exception {
        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                (Function<String, String>) (input, context) -> input + "-lambda",
                new InstanceConfig());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
        assertNotNull(result.getResult());
        assertEquals(new String(testString + "-lambda"), result.getResult());
        instance.close();
    }
    
    @Test
    public void testNullReturningFunction() {
    	JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                (Function<String, String>) (input, context) -> null,
                new InstanceConfig());
    	String testString = "ABC123";
    	JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
    	assertNull(result.getResult());
    	instance.close();
    }
    
    @Test
    public void testUserExceptionThrowingFunction() {
    	Function<String, String> func = (input, context) -> {
    		throw new UserException("Boom");
    	};
    	
    	JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                func,
                new InstanceConfig());
    	String testString = "ABC123";
    	JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
    	assertNull(result.getResult());
    	assertNotNull(result.getUserException());
    	instance.close();
    }
    
    @Test
    public void testSystemExceptionThrowingFunction() {
    	Function<String, String> func = (input, context) -> {
    		throw new InterruptedException("Boom");
    	};
    	
    	JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                func,
                new InstanceConfig());
    	String testString = "ABC123";
    	JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
    	assertNull(result.getResult());
    	assertNotNull(result.getUserException());
    	instance.close();
    }

    @Test
    public void testAsyncFunction() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        Function<String, CompletableFuture<String>> function = (input, context) -> {
            log.info("input string: {}", input);
            CompletableFuture<String> result  = new CompletableFuture<>();
            executor.submit(() -> {
                try {
                    Thread.sleep(500);
                    result.complete(String.format("%s-lambda", input));
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            });

            return result;
        };

        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                function,
                instanceConfig);
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
        assertNotNull(result.getResult());
        assertEquals(((CompletableFuture)result.getResult()).get(), new String(testString + "-lambda"));
        instance.close();
    }
    
    @Test
    public void testNullReturningAsyncFunction() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        Function<String, CompletableFuture<String>> function = (input, context) -> {
            log.info("input string: {}", input);
            CompletableFuture<String> result  = new CompletableFuture<>();
            executor.submit(() -> {
                try {
                    Thread.sleep(500);
                    result.complete(null);
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            });

            return result;
        };

        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                function,
                instanceConfig);
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
        assertNotNull(result.getResult());
        instance.close();
    }
    
    @Test
    public void testSystemExceptionThrowingAsyncFunction() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        Function<String, CompletableFuture<String>> function = (input, context) -> {
            log.info("input string: {}", input);
            CompletableFuture<String> result  = new CompletableFuture<>();
            executor.submit(() -> {
            	result.completeExceptionally(new InterruptedException(""));
            });

            return result;
        };

        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                function,
                instanceConfig);
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
        assertNotNull(result.getResult());
        
        awaitCompletion((CompletableFuture)result.getResult());
        assertTrue(((CompletableFuture)result.getResult()).isCompletedExceptionally());
        assertNotNull(result.getSystemException());
        instance.close();
    }
    
    @Test
    public void testUserExceptionThrowingAsyncFunction() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        Function<String, CompletableFuture<String>> function = (input, context) -> {
            log.info("input string: {}", input);
            CompletableFuture<String> result  = new CompletableFuture<>();
            executor.submit(() -> {
            	result.completeExceptionally(new UserException("Boom"));
            });

            return result;
        };

        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                function,
                instanceConfig);
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
        assertNotNull(result.getResult());
        awaitCompletion((CompletableFuture)result.getResult());
        assertNotNull(result.getUserException());
        instance.close();
    }

    @Test
    public void testAsyncFunctionMaxPending() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        int pendingQueueSize = 3;
        instanceConfig.setMaxPendingAsyncRequests(pendingQueueSize);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        Function<String, CompletableFuture<String>> function = (input, context) -> {
            log.info("input string: {}", input);
            CompletableFuture<String> result  = new CompletableFuture<>();
            executor.submit(() -> {
                try {
                    Thread.sleep(500);
                    result.complete(String.format("%s-lambda", input));
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            });

            return result;
        };

        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                function,
                instanceConfig);
        String testString = "ABC123";

        long startTime = System.currentTimeMillis();
        assertEquals(pendingQueueSize, instance.getPendingAsyncRequests().remainingCapacity());
        JavaExecutionResult result1 = instance.handleMessage(mock(Record.class), testString);
        assertEquals(pendingQueueSize - 1, instance.getPendingAsyncRequests().remainingCapacity());
        JavaExecutionResult result2 = instance.handleMessage(mock(Record.class), testString);
        assertEquals(pendingQueueSize - 2, instance.getPendingAsyncRequests().remainingCapacity());
        JavaExecutionResult result3 = instance.handleMessage(mock(Record.class), testString);
        // no space left
        assertEquals(0, instance.getPendingAsyncRequests().remainingCapacity());

        instance.getPendingAsyncRequests().remainingCapacity();
        assertNotNull(result1.getResult());
        assertNotNull(result2.getResult());
        assertNotNull(result3.getResult());

        assertEquals(((CompletableFuture)result1.getResult()).get(), new String(testString + "-lambda"));
        long endTime = System.currentTimeMillis();

        log.info("start:{} end:{} during:{}", startTime, endTime, endTime - startTime);
        instance.close();
    }
    
    private static void awaitCompletion(CompletableFuture future) {
    	try {
            Thread.sleep(1000);
            future.join();
            Thread.sleep(1000);  // Give the whenCompleteAsync stage some time to execute
          } catch (final Exception ex) {
          	// Ignore
          }
    }
    
    private static class UserException extends Exception {
    	public UserException(String msg) {
    		super(msg);
    	}
    }
}
