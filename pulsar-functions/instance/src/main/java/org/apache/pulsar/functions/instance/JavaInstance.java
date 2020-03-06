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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.RichFunction;

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
    private RichFunction function;
    private java.util.function.Function javaUtilFunction;

    public JavaInstance(ContextImpl contextImpl, Object userClassObject) {

        this.context = contextImpl;

        // create the functions
        if (userClassObject instanceof RichFunction) {
            this.function = (RichFunction) userClassObject;
        } else if (userClassObject instanceof Function) {
            this.function = new DefaultRichFunctionWrapper((Function) userClassObject);
        } else {
            this.function = new DefaultRichFunctionWrapper((java.util.function.Function) userClassObject);
        }
    }

    public JavaExecutionResult handleMessage(Record<?> record) {
        if (context != null) {
            context.setCurrentMessageContext(record);
        }
        JavaExecutionResult executionResult = new JavaExecutionResult();
        try {
            executionResult.setResult(this.function.process(record, context));
        } catch (Exception ex) {
            executionResult.setUserException(ex);
        }
        return executionResult;
    }

    @Override
    public void close() {
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
