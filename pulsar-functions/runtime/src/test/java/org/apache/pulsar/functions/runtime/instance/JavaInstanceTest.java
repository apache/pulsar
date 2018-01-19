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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.api.utils.JavaSerDe;
import org.apache.pulsar.functions.api.utils.Utf8StringSerDe;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;

public class JavaInstanceTest {

    private class LongRunningHandler implements PulsarFunction<String, String> {
        @Override
        public String process(String input, Context context) throws Exception {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {

            }
            return input;
        }
    }

    private byte[] serialize(String resultValue) {
        return Utf8StringSerDe.of().serialize(resultValue);
    }

    @Getter
    @Setter
    private class UnSupportedClass {
        private String name;
        private Integer age;
    }

    private class UnsupportedHandler implements PulsarFunction<String, UnSupportedClass> {
        @Override
        public UnSupportedClass process(String input, Context context) throws Exception {
            return new UnSupportedClass();
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

    private static InstanceConfig createInstanceConfig() {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        LimitsConfig limitsConfig = new LimitsConfig();
        functionConfigBuilder.putInputs("TEST", Utf8StringSerDe.class.getName());
        functionConfigBuilder.setOutputSerdeClassName(Utf8StringSerDe.class.getName());
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionConfig(functionConfigBuilder.build());
        instanceConfig.setLimitsConfig(limitsConfig);
        return instanceConfig;
    }

    /**
     * Verify that functions running longer than time budget fails with Timeout exception
     * @throws Exception
     */
    @Test
    public void testLongRunningFunction() throws Exception {
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        JavaInstance instance = new JavaInstance(
            config, new LongRunningHandler(), null, null, Arrays.asList(Utf8StringSerDe.of()), Utf8StringSerDe.of(), new HashMap<>());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(MessageId.earliest, "random", testString);

        assertNull(result.getUserException());
        assertNotNull(result.getTimeoutException());
    }

    /**
     * Verify that be able to run lambda functions.
     * @throws Exception
     */
    @Test
    public void testLambda() {
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        JavaInstance instance = new JavaInstance(
            config,
            (PulsarFunction<String, String>) (input, context) -> input + "-lambda",
            null, null,
            Arrays.asList(Utf8StringSerDe.of()), Utf8StringSerDe.of(), new HashMap<>());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(MessageId.earliest, "random", testString);
        assertNotNull(result.getResult());
        assertEquals(new String(testString + "-lambda"), result.getResult());
    }

    /**
     * Verify that JavaInstance does not support functions that are not native Java types
     * @throws Exception
     */
    @Test
    public void testUnsupportedClasses() {
        InstanceConfig config = createInstanceConfig();
        try {
            new JavaInstance(
                config, new UnsupportedHandler(), null, null, Arrays.asList(Utf8StringSerDe.of()), Utf8StringSerDe.of(), new HashMap<>());
            assertFalse(true);
        } catch (RuntimeException ex) {
            // Good
        } catch (Exception ex) {
            assertFalse(true);
        }
    }

    /**
     * Verify that JavaInstance does not support functions that take Void type as input
     */
    @Test
    public void testVoidInputClasses() {
        InstanceConfig config = createInstanceConfig();
        try {
            new JavaInstance(
                config, new VoidInputHandler(), null, null, Arrays.asList(Utf8StringSerDe.of()), null, new HashMap<>());
            assertFalse(true);
        } catch (RuntimeException ex) {
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
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        JavaInstance instance = new JavaInstance(
            config, new VoidOutputHandler(), null, null, Arrays.asList(Utf8StringSerDe.of()), Utf8StringSerDe.of(), new HashMap<>());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(MessageId.earliest, "r", testString);
        assertNull(result.getUserException());
        assertNull(result.getTimeoutException());
        assertNull(result.getResult());
    }

    /**
     * Verify that function input type should be consistent with input serde type.
     */
    @Test
    public void testInconsistentInputType() {
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        config.setFunctionConfig(FunctionConfig.newBuilder(config.getFunctionConfig())
                .putInputs("TEST", JavaSerDe.class.getName()).build());

        try {
            new JavaInstance(
                config,
                (PulsarFunction<String, String>) (input, context) -> input + "-lambda",
                    null, null,
                    Arrays.asList(JavaSerDe.of()), Utf8StringSerDe.of(), new HashMap<>());
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException re) {
            assertTrue(re.getMessage().startsWith("Inconsistent types found between function input type and input serde type:"));
        }
    }

    /**
     * Verify that function output type should be consistent with output serde type.
     */
    @Test
    public void testInconsistentOutputType() {
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        config.setFunctionConfig(FunctionConfig.newBuilder(config.getFunctionConfig())
                .setOutputSerdeClassName(JavaSerDe.class.getName()).build());

        try {
            new JavaInstance(
                config,
                (PulsarFunction<String, String>) (input, context) -> input + "-lambda",
                    null, null,
                    Arrays.asList(Utf8StringSerDe.of()), JavaSerDe.of(), new HashMap<>());
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException re) {
            assertTrue(re.getMessage().startsWith("Inconsistent types found between function output type and output serde type:"));
        }
    }
}
