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

import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.api.RawRequestHandler;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.utils.Reflections;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Java Instance. This is started by the spawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
@Slf4j
public class JavaInstance implements AutoCloseable {

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

    private ContextImpl context;
    private RequestHandler requestHandler;
    private RawRequestHandler rawRequestHandler;
    private ExecutorService executorService;

    public JavaInstance(JavaInstanceConfig config, ClassLoader clsLoader,
                        PulsarClient pulsarClient,
                        List<SerDe> inputSerDe, SerDe outputSerDe) {
        this(
            config,
            Reflections.createInstance(
                config.getFunctionConfig().getClassName(),
                clsLoader), clsLoader, pulsarClient, inputSerDe, outputSerDe);
    }

    JavaInstance(JavaInstanceConfig config, Object object,
                 ClassLoader clsLoader,
                 PulsarClient pulsarClient,
                 List<SerDe> inputSerDe, SerDe outputSerDe) {
        // TODO: cache logger instances by functions?
        Logger instanceLog = LoggerFactory.getLogger("function-" + config.getFunctionConfig().getName());

        this.context = new ContextImpl(config, instanceLog, pulsarClient, clsLoader);

        // create the functions
        if (object instanceof RequestHandler) {
            requestHandler = (RequestHandler) object;
            computeInputAndOutputTypesAndVerifySerDe(inputSerDe, outputSerDe);
        } else if (object instanceof RawRequestHandler) {
            rawRequestHandler = (RawRequestHandler) object;
        } else {
            throw new RuntimeException("User class must be either a Request or Raw Request Handler");
        }

        if (config.getLimitsConfig() != null && config.getLimitsConfig().getMaxTimeMs() > 0) {
            log.info("Spinning up a executor service since time budget is infinite");
            executorService = Executors.newFixedThreadPool(1);
        }
    }

    private void computeInputAndOutputTypesAndVerifySerDe(List<SerDe> inputSerDe, SerDe outputSerDe) {
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(RequestHandler.class, requestHandler.getClass());
        verifySupportedType(typeArgs[0], false);
        verifySupportedType(typeArgs[1], true);

        for (SerDe serDe : inputSerDe) {
            Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
            verifySupportedType(inputSerdeTypeArgs[0], false);
            if (!typeArgs[0].equals(inputSerdeTypeArgs[0])) {
                throw new RuntimeException("Inconsistent types found between function input type and input serde type: "
                        + " function type = " + typeArgs[0] + ", serde type = " + inputSerdeTypeArgs[0]);
            }
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

    public JavaExecutionResult handleMessage(String messageId, String topicName, byte[] data, SerDe inputSerDe) {
        context.setCurrentMessageContext(messageId, topicName);
        JavaExecutionResult executionResult = new JavaExecutionResult();
        if (executorService == null) {
            return processMessage(executionResult, data, inputSerDe);
        }
        Future<?> future = executorService.submit(() -> processMessage(executionResult, data, inputSerDe));
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

    private JavaExecutionResult processMessage(JavaExecutionResult executionResult, byte[] data,
                                               SerDe inputSerDe) {
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
        return executionResult;
    }

    @Override
    public void close() {
        if (null != executorService) {
            executorService.shutdown();
        }
    }

    public InstanceCommunication.MetricsData getAndResetMetrics() {
        return context.getAndResetMetrics();
    }
}
