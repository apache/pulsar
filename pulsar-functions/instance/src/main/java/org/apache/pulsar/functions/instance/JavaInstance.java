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

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

import java.lang.reflect.Method;
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
    private Object function;
//    private Object javaUtilFunction;

    private Method functionProcess;
    private Method functionApply;

    public JavaInstance(ContextImpl contextImpl, Object userClassObject) {
        this.context = contextImpl;
        this.function = userClassObject;
    }

    public void initialize() throws NoSuchMethodException, ClassNotFoundException {
        if (InstanceUtils.isAssignable(function.getClass(), Function.class)) {
            functionProcess = function.getClass().getMethod("process", Object.class,
                    Thread.currentThread().getContextClassLoader().loadClass(Context.class.getName()));
        } else {
            functionApply = function.getClass().getMethod("apply", Object.class);
        }
    }

    public JavaExecutionResult handleMessage(Record<?> record, Object input) {
        if (context != null) {
            context.setCurrentMessageContext(record);
        }
        JavaExecutionResult executionResult = new JavaExecutionResult();
        try {
            Object output;

//            Method method = function.getClass().getDeclaredMethod("process", Object.class,
//                    Thread.currentThread().getContextClassLoader().loadClass("org.apache.pulsar.functions.api.Context"));
//            output = method.invoke(function, input, null);

            if (functionProcess != null) {
                log.info("functionProcess: {}", functionProcess);
                log.info("function: {} - {} - {} - {}", function, function.getClass().getClassLoader(), context.getClass().getClassLoader(), Thread.currentThread().getContextClassLoader());
                log.info("function.getClass().getClassLoader(): {}", getClassloaderHierarchy(function.getClass().getClassLoader()));
                log.info("context.getClass().getClassLoader()): {}", getClassloaderHierarchy(context.getClass().getClassLoader()));
                log.info("Thread.currentThread().getContextClassLoader(): {}", getClassloaderHierarchy(Thread.currentThread().getContextClassLoader()));

                output = functionProcess.invoke(function, input, context);
            } else {
                output = functionApply.invoke(function, input);
            }

//            if (function != null) {
//
//                output = function.process(input, context);
//            } else {
//                output = javaUtilFunction.apply(input);
//            }
            executionResult.setResult(output);
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

    public static String getClassloaderHierarchy(ClassLoader classLoader) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Root: %s\n", classLoader));
        ClassLoader c = classLoader;
        while(true) {
            c = c.getParent();
            if (c != null) {
                sb.append(String.format(" - > %s\n", c));
            } else {
                break;
            }
        }
        return sb.toString();
    }
}
