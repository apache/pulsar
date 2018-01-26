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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.apache.pulsar.functions.utils.Reflections;

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

    @Getter(AccessLevel.PACKAGE)
    private final ContextImpl context;
    private PulsarFunction pulsarFunction;
    private ExecutorService executorService;

    public JavaInstance(InstanceConfig config, ClassLoader clsLoader,
                        PulsarClient pulsarClient,
                        List<SerDe> inputSerDe,
                        SerDe outputSerDe,
                        Map<String, Consumer> sourceConsumers) {
        this(
            config,
            Reflections.createInstance(
                config.getFunctionConfig().getClassName(),
                clsLoader), clsLoader, pulsarClient, inputSerDe, outputSerDe, sourceConsumers);
    }

    JavaInstance(InstanceConfig config, Object object,
                 ClassLoader clsLoader,
                 PulsarClient pulsarClient,
                 List<SerDe> inputSerDe, SerDe outputSerDe, Map<String, Consumer> sourceConsumers) {
        // TODO: cache logger instances by functions?
        Logger instanceLog = LoggerFactory.getLogger("function-" + config.getFunctionConfig().getName());

        this.context = new ContextImpl(config, instanceLog, pulsarClient, clsLoader, sourceConsumers);

        // create the functions
        if (object instanceof PulsarFunction) {
            pulsarFunction = (PulsarFunction) object;
            verifyTypes(inputSerDe, outputSerDe);
        } else {
            throw new RuntimeException("User class must be either a Request or Raw Request Handler");
        }

        if (config.getLimitsConfig() != null && config.getLimitsConfig().getMaxTimeMs() > 0) {
            log.info("Spinning up a executor service since time budget is infinite");
            executorService = Executors.newFixedThreadPool(1);
        }
    }

    private void verifyTypes(List<SerDe> inputSerDe, SerDe outputSerDe) {
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());

        for (SerDe serDe : inputSerDe) {
            Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
            if (!typeArgs[0].equals(inputSerdeTypeArgs[0])) {
                throw new RuntimeException("Inconsistent types found between function input type and input serde type: "
                        + " function type = " + typeArgs[0] + ", serde type = " + inputSerdeTypeArgs[0]);
            }
        }

        if (!Void.class.equals(typeArgs[1])) { // return type is not `Void.class`
            if (outputSerDe == null) {
                throw new RuntimeException("Output serde class is null even though return type is not Void!");
            }
            Class<?>[] outputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, outputSerDe.getClass());
            if (!typeArgs[1].equals(outputSerdeTypeArgs[0])) {
                throw new RuntimeException("Inconsistent types found between function output type and output serde type: "
                    + " function type = " + typeArgs[1] + ", serde type = " + outputSerdeTypeArgs[0]);
            }
        }
    }

    public JavaExecutionResult handleMessage(MessageId messageId, String topicName, Object input) {
        context.setCurrentMessageContext(messageId, topicName);
        JavaExecutionResult executionResult = new JavaExecutionResult();
        if (executorService == null) {
            return processMessage(executionResult, input);
        }
        Future<?> future = executorService.submit(() -> processMessage(executionResult, input));
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

    private JavaExecutionResult processMessage(JavaExecutionResult executionResult, Object input) {

        try {
            Object output = pulsarFunction.process(input, context);
            executionResult.setResult(output);
        } catch (Exception ex) {
            executionResult.setUserException(ex);
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
