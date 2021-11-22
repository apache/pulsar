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
import static org.testng.Assert.assertSame;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.JavaInstance.AsyncFuncRequest;
import org.testng.Assert;
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
        assertEquals(testString + "-lambda", result.getResult());
        instance.close();
    }
    
    @Test
    public void testNullReturningFunction() throws Exception  {
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
    public void testUserExceptionThrowingFunction() throws Exception  {
    	final UserException userException = new UserException("Boom");
    	Function<String, String> func = (input, context) -> {
    		throw userException;
    	};

    	JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                func,
                new InstanceConfig());
    	String testString = "ABC123";
    	JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString);
    	assertSame(userException, result.getUserException());
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
        CompletableFuture<JavaExecutionResult> resultHolder = new CompletableFuture<>();
        JavaExecutionResult result = instance.handleMessage(
            mock(Record.class), testString,
            (record, javaResult) -> resultHolder.complete(javaResult), cause -> {});
        assertNull(result);
        assertNotNull(resultHolder.get());
        assertEquals(testString + "-lambda", resultHolder.get().getResult());
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
        CompletableFuture<JavaExecutionResult> resultHolder = new CompletableFuture<>();
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString,
            (record, javaResult) -> resultHolder.complete(javaResult), cause -> {});
        assertNull(result);
        assertNotNull(resultHolder.get());
        instance.close();
    }

    @Test
    public void testUserExceptionThrowingAsyncFunction() throws Exception {
    	final UserException userException = new UserException("Boom");
        InstanceConfig instanceConfig = new InstanceConfig();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        Function<String, CompletableFuture<String>> function = (input, context) -> {
            log.info("input string: {}", input);
            CompletableFuture<String> result  = new CompletableFuture<>();
            executor.submit(() -> {
            	result.completeExceptionally(userException);
            });

            return result;
        };

        JavaInstance instance = new JavaInstance(
                mock(ContextImpl.class),
                function,
                instanceConfig);
        String testString = "ABC123";
        CompletableFuture<JavaExecutionResult> resultHolder = new CompletableFuture<>();
        JavaExecutionResult result = instance.handleMessage(mock(Record.class), testString,
            (record, javaResult) -> resultHolder.complete(javaResult), cause -> {});
        assertNull(result);
        assertSame(userException, resultHolder.get().getUserException());
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
        assertNull(instance.handleMessage(mock(Record.class), testString));
        assertEquals(pendingQueueSize - 1, instance.getPendingAsyncRequests().remainingCapacity());
        assertNull(instance.handleMessage(mock(Record.class), testString));
        assertEquals(pendingQueueSize - 2, instance.getPendingAsyncRequests().remainingCapacity());
        assertNull(instance.handleMessage(mock(Record.class), testString));
        // no space left
        assertEquals(0, instance.getPendingAsyncRequests().remainingCapacity());

        for (int i = 0; i < 3; i++) {
            AsyncFuncRequest request = instance.getPendingAsyncRequests().poll();
            Assert.assertEquals(request.getProcessResult().get(), testString + "-lambda");
        }

        long endTime = System.currentTimeMillis();

        log.info("start:{} end:{} during:{}", startTime, endTime, endTime - startTime);
        instance.close();
    }
    
	private static class UserException extends Exception {
    	public UserException(String msg) {
    		super(msg);
    	}
    }
}
