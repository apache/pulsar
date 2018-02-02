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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;

import java.util.Map;
import java.util.function.Function;

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
    private Function javaUtilFunction;
    private ExecutorService executorService;

    public JavaInstance(InstanceConfig config, Object userClassObject,
                 ClassLoader clsLoader,
                 PulsarClient pulsarClient,
                 Map<String, Consumer> sourceConsumers) {
        // TODO: cache logger instances by functions?
        Logger instanceLog = LoggerFactory.getLogger("function-" + config.getFunctionConfig().getName());

        this.context = new ContextImpl(config, instanceLog, pulsarClient, clsLoader, sourceConsumers);

        // create the functions
        if (userClassObject instanceof PulsarFunction) {
            this.pulsarFunction = (PulsarFunction) userClassObject;
        } else {
            this.javaUtilFunction = (Function) userClassObject;
        }

        if (config.getLimitsConfig() != null && config.getLimitsConfig().getMaxTimeMs() > 0) {
            log.info("Spinning up a executor service since time budget is infinite");
            executorService = Executors.newFixedThreadPool(1);
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
            Object output;
            if (pulsarFunction != null) {
                output = pulsarFunction.process(input, context);
            } else {
                output = javaUtilFunction.apply(input);
            }
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
