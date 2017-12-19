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
package org.apache.pulsar.functions.runtime.instance;

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.api.RawRequestHandler;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.runtime.container.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * This is the Java Instance. This is started by the spawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
public class JavaInstance {
    private static final Logger log = LoggerFactory.getLogger(JavaInstance.class);
    private final List<Type> supportedInputTypes = Arrays.asList(
            Integer.TYPE,
            Double.TYPE,
            Long.TYPE,
            String.class,
            Short.TYPE,
            Byte.TYPE,
            Float.TYPE,
            Map.class,
            List.class
    );
    private ContextImpl context;
    private RequestHandler requestHandler;
    private RawRequestHandler rawRequestHandler;
    private ExecutorService executorService;
    private SerDe serDe;

    public static Object createObject(String userClassName) {
        Object object;
        try {
            Class<?> clazz = Class.forName(userClassName);
            object = clazz.newInstance();
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex + " User class must be in class path.");
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex + " User class must be concrete.");
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex + " User class must have a no-arg constructor.");
        }
        return object;
    }

    public JavaInstance(JavaInstanceConfig config) {
        this(config, createObject(config.getFunctionConfig().getClassName()));
    }

    public JavaInstance(JavaInstanceConfig config, Object object) {
        this.context = new ContextImpl(config, log);
        if (object instanceof RequestHandler) {
            requestHandler = (RequestHandler) object;
            computeInputAndOutputTypes();
        } else if (object instanceof RawRequestHandler) {
            rawRequestHandler = (RawRequestHandler) object;
        } else {
            throw new RuntimeException("User class must be either a Request or Raw Request Handler");
        }

        executorService = Executors.newFixedThreadPool(1);
        this.serDe = config.getSerDe();
    }

    private void computeInputAndOutputTypes() {
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(RequestHandler.class, requestHandler.getClass());
        verifySupportedType(typeArgs[0], false);
        verifySupportedType(typeArgs[1], true);
    }

    private void verifySupportedType(Type type, boolean allowVoid) {
        if (!allowVoid && !supportedInputTypes.contains(type)) {
            throw new RuntimeException("Non Basic types not yet supported: " + type);
        } else if (!(supportedInputTypes.contains(type) || type.equals(Void.class))) {
            throw new RuntimeException("Non Basic types not yet supported: " + type);
        }
    }

    public JavaExecutionResult handleMessage(String messageId, String topicName, byte[] data) {
        context.setCurrentMessageContext(messageId, topicName);
        JavaExecutionResult executionResult = new JavaExecutionResult();
        Future<?> future = executorService.submit(() -> {
            if (requestHandler != null) {
                try {
                    Object input = serDe.deserialize(data);
                    Object output = requestHandler.handleRequest(input, context);
                    executionResult.setResult(output);
                } catch (Exception ex) {
                    executionResult.setUserException(ex);
                }
            } else if (rawRequestHandler != null) {
                try {
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    rawRequestHandler.handleRequest(inputStream, outputStream, context);
                    executionResult.setResult(outputStream.toByteArray());
                } catch (Exception ex) {
                    executionResult.setUserException(ex);
                }
            }
        });
        try {
            future.get(context.getTimeBudgetInMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("handleMessage was interrupted");
            executionResult.setUserException(e);
        } catch (ExecutionException e) {
            log.error("handleMessage threw exception: " + e.getCause());
            executionResult.setUserException(e);
        } catch (TimeoutException e) {
            future.cancel(true);              //     <-- interrupt the job
            log.error("handleMessage timed out");
            executionResult.setTimeoutException(e);
        }

        return executionResult;
    }
}
