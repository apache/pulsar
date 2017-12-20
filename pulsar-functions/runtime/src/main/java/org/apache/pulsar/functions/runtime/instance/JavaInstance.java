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

import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.api.RawRequestHandler;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.runtime.serde.SerDe;
import org.apache.pulsar.functions.utils.Reflections;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * This is the Java Instance. This is started by the spawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
@Slf4j
public class JavaInstance {

    private static final Set<Type> supportedInputTypes = Sets.newHashSet(
        Integer.TYPE,
        Double.TYPE,
        Long.TYPE,
        String.class,
        Short.TYPE,
        Byte.TYPE,
        Float.TYPE,
        Map.class,
        List.class,
        Object.class
    );

    private static SerDe initializeSerDe(String serdeClassName, ClassLoader classLoader) {
        if (null == serdeClassName) {
            return null;
        } else {
            return Reflections.createInstance(
                serdeClassName,
                SerDe.class,
                classLoader);
        }
    }

    private ContextImpl context;
    private RequestHandler requestHandler;
    private RawRequestHandler rawRequestHandler;
    private ExecutorService executorService;
    private SerDe inputSerDe;
    @Getter
    private SerDe outputSerDe;

    public JavaInstance(JavaInstanceConfig config) {
        this(config, Thread.currentThread().getContextClassLoader());
    }

    public JavaInstance(JavaInstanceConfig config, ClassLoader clsLoader) {
        this(
            config,
            Reflections.createInstance(
                config.getFunctionConfig().getClassName(),
                clsLoader),
            clsLoader);
    }

    JavaInstance(JavaInstanceConfig config, Object object, ClassLoader clsLoader) {
        this.context = new ContextImpl(config, log);

        // create the serde
        this.inputSerDe = initializeSerDe(config.getFunctionConfig().getInputSerdeClassName(), clsLoader);
        this.outputSerDe = initializeSerDe(config.getFunctionConfig().getOutputSerdeClassName(), clsLoader);
        // create the functions
        if (object instanceof RequestHandler) {
            requestHandler = (RequestHandler) object;
            computeInputAndOutputTypesAndVerifySerDe();
        } else if (object instanceof RawRequestHandler) {
            rawRequestHandler = (RawRequestHandler) object;
        } else {
            throw new RuntimeException("User class must be either a Request or Raw Request Handler");
        }

        executorService = Executors.newFixedThreadPool(1);
    }

    private void computeInputAndOutputTypesAndVerifySerDe() {
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(RequestHandler.class, requestHandler.getClass());
        verifySupportedType(typeArgs[0], false);
        verifySupportedType(typeArgs[1], true);

        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, inputSerDe.getClass());
        verifySupportedType(inputSerdeTypeArgs[0], false);
        if (!typeArgs[0].equals(inputSerdeTypeArgs[0])) {
            throw new RuntimeException("Inconsistent types found between function input type and input serde type: "
                + " function type = " + typeArgs[0] + ", serde type = " + inputSerdeTypeArgs[0]);
        }

        if (!Void.class.equals(typeArgs[1])) { // return type is not `Void.class`
            Class<?>[] outputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, outputSerDe.getClass());
            verifySupportedType(outputSerdeTypeArgs[0], false);
            if (!typeArgs[1].equals(outputSerdeTypeArgs[0])) {
                throw new RuntimeException("Inconsistent types found between function output type and output serde type: "
                    + " function type = " + typeArgs[1] + ", serde type = " + outputSerdeTypeArgs[0]);
            }
        }
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
                    Object input = inputSerDe.deserialize(data);
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
            executionResult.setSystemException(e);
        } catch (ExecutionException e) {
            log.error("handleMessage threw exception: " + e.getCause());
            executionResult.setSystemException(e);
        } catch (TimeoutException e) {
            future.cancel(true);              //     <-- interrupt the job
            log.error("handleMessage timed out");
            executionResult.setTimeoutException(e);
        }

        return executionResult;
    }
}
