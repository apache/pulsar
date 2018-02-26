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

import lombok.Getter;
import lombok.Setter;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.testng.AssertJUnit.*;

public class JavaInstanceRunnableTest {

    static class IntegerSerDe implements SerDe<Integer> {
        @Override
        public Integer deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(Integer input) {
            return new byte[0];
        }
    }

    private static InstanceConfig createInstanceConfig(boolean addCustom, String outputSerde) {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        if (!addCustom) {
            functionConfigBuilder.addInputs("TEST");
        } else {
            functionConfigBuilder.putCustomSerdeInputs("TEST", IntegerSerDe.class.getName());
        }
        if (outputSerde != null) {
            functionConfigBuilder.setOutputSerdeClassName(outputSerde);
        }
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionConfig(functionConfigBuilder.build());
        instanceConfig.setMaxBufferedTuples(1024);
        return instanceConfig;
    }

    private JavaInstanceRunnable createRunnable(boolean addCustom, String outputSerde) throws Exception {
        InstanceConfig config = createInstanceConfig(addCustom, outputSerde);
        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                config, null, null, null, null);
        return javaInstanceRunnable;
    }

    private Method makeAccessible(JavaInstanceRunnable javaInstanceRunnable) throws Exception {
        Method method = javaInstanceRunnable.getClass().getDeclaredMethod("setupSerDe", Class[].class, ClassLoader.class);
        method.setAccessible(true);
        return method;
    }

    @Getter
    @Setter
    private class ComplexUserDefinedType {
        private String name;
        private Integer age;
    }

    private class ComplexTypeHandler implements PulsarFunction<String, ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType process(String input, Context context) throws Exception {
            return new ComplexUserDefinedType();
        }
    }

    private class ComplexSerDe implements SerDe<ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(ComplexUserDefinedType input) {
            return new byte[0];
        }
    }

    private class VoidInputHandler implements PulsarFunction<Void, String> {
        @Override
        public String process(Void input, Context context) throws Exception {
            return new String("Interesting");
        }
    }

    private class VoidOutputHandler implements PulsarFunction<String, Void> {
        @Override
        public Void process(String input, Context context) throws Exception {
            return null;
        }
    }

    /**
     * Verify that JavaInstance does not support functions that take Void type as input
     */
    @Test
    public void testVoidInputClasses() {
        try {
            JavaInstanceRunnable runnable = createRunnable(false, DefaultSerDe.class.getName());
            Method method = makeAccessible(runnable);
            VoidInputHandler pulsarFunction = new VoidInputHandler();
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            method.invoke(runnable, typeArgs, clsLoader);
            assertFalse(true);
        } catch (InvocationTargetException ex) {
            // Good
        } catch (Exception ex) {
            assertFalse(true);
        }
    }

    /**
     * Verify that JavaInstance does support functions that output Void type
     */
    @Test
    public void testVoidOutputClasses() {
        try {
            JavaInstanceRunnable runnable = createRunnable(false, DefaultSerDe.class.getName());
            Method method = makeAccessible(runnable);
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            VoidOutputHandler pulsarFunction = new VoidOutputHandler();
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            method.invoke(runnable, typeArgs, clsLoader);
        } catch (Exception ex) {
            assertTrue(false);
        }
    }

    /**
     * Verify that function input type should be consistent with input serde type.
     */
    @Test
    public void testInconsistentInputType() {
        try {
            JavaInstanceRunnable runnable = createRunnable(true, DefaultSerDe.class.getName());
            Method method = makeAccessible(runnable);
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            PulsarFunction pulsarFunction = (PulsarFunction<String, String>) (input, context) -> input + "-lambda";
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            method.invoke(runnable, typeArgs, clsLoader);
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (InvocationTargetException ex) {
            assertTrue(ex.getCause().getMessage().startsWith("Inconsistent types found between function input type and input serde type:"));
        } catch (Exception ex) {
            assertTrue(false);
        }
    }

    /**
     * Verify that Default Serializer works fine.
     */
    @Test
    public void testDefaultSerDe() {
        try {
            JavaInstanceRunnable runnable = createRunnable(false, null);
            Method method = makeAccessible(runnable);
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            PulsarFunction pulsarFunction = (PulsarFunction<String, String>) (input, context) -> input + "-lambda";
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            method.invoke(runnable, typeArgs, clsLoader);
        } catch (Exception ex) {
            ex.printStackTrace();
            assertEquals(ex, null);
            assertTrue(false);
        }
    }

    /**
     * Verify that Explicit setting of Default Serializer works fine.
     */
    @Test
    public void testExplicitDefaultSerDe() {
        try {
            JavaInstanceRunnable runnable = createRunnable(false, DefaultSerDe.class.getName());
            Method method = makeAccessible(runnable);
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            PulsarFunction pulsarFunction = (PulsarFunction<String, String>) (input, context) -> input + "-lambda";
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            method.invoke(runnable, typeArgs, clsLoader);
        } catch (Exception ex) {
            assertTrue(false);
        }
    }

    /**
     * Verify that function output type should be consistent with output serde type.
     */
    @Test
    public void testInconsistentOutputType() {
        try {
            JavaInstanceRunnable runnable = createRunnable(false, IntegerSerDe.class.getName());
            Method method = makeAccessible(runnable);
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            PulsarFunction pulsarFunction = (PulsarFunction<String, String>) (input, context) -> input + "-lambda";
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            method.invoke(runnable, typeArgs, clsLoader);
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (InvocationTargetException ex) {
            assertTrue(ex.getCause().getMessage().startsWith("Inconsistent types found between function output type and output serde type:"));
        } catch (Exception ex) {
            assertTrue(false);
        }
    }


}
