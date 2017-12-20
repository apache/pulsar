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
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.serde.JavaSerDe;
import org.apache.pulsar.functions.runtime.serde.Utf8StringSerDe;
import org.testng.annotations.Test;

public class JavaInstanceTest {

    private class LongRunningHandler implements RequestHandler<String, String> {
        @Override
        public String handleRequest(String input, Context context) throws Exception {
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

    private class UnsupportedHandler implements RequestHandler<String, UnSupportedClass> {
        @Override
        public UnSupportedClass handleRequest(String input, Context context) throws Exception {
            return new UnSupportedClass();
        }
    }

    private class VoidInputHandler implements RequestHandler<Void, String> {
        @Override
        public String handleRequest(Void input, Context context) throws Exception {
            return new String("Interesting");
        }
    }

    private class VoidOutputHandler implements RequestHandler<String, Void> {
        @Override
        public Void handleRequest(String input, Context context) throws Exception {
            return null;
        }
    }

    private static JavaInstanceConfig createInstanceConfig() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setSerdeClassName(Utf8StringSerDe.class.getName());
        JavaInstanceConfig instanceConfig = new JavaInstanceConfig();
        instanceConfig.setFunctionConfig(functionConfig);
        return instanceConfig;
    }

    /**
     * Verify that functions running longer than time budget fails with Timeout exception
     * @throws Exception
     */
    @Test
    public void testLongRunningFunction() throws Exception {
        JavaInstanceConfig config = createInstanceConfig();
        config.setTimeBudgetInMs(2000);
        JavaInstance instance = new JavaInstance(
            config, new LongRunningHandler(), Thread.currentThread().getContextClassLoader());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage("1", "random", serialize(testString));

        assertNull(result.getUserException());
        assertNotNull(result.getTimeoutException());
    }

    /**
     * Verify that be able to run lambda functions.
     * @throws Exception
     */
    @Test
    public void testLambda() {
        JavaInstanceConfig config = createInstanceConfig();
        config.setTimeBudgetInMs(2000);
        JavaInstance instance = new JavaInstance(
            config,
            (RequestHandler<String, String>) (input, context) -> input + "-lambda",
            Thread.currentThread().getContextClassLoader());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage("1", "random", serialize(testString));
        assertNotNull(result.getResult());
        assertEquals(new String(testString + "-lambda"), result.getResult());
    }

    /**
     * Verify that JavaInstance does not support functions that are not native Java types
     * @throws Exception
     */
    @Test
    public void testUnsupportedClasses() {
        JavaInstanceConfig config = createInstanceConfig();
        try {
            new JavaInstance(
                config, new UnsupportedHandler(), Thread.currentThread().getContextClassLoader());
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
        JavaInstanceConfig config = createInstanceConfig();
        try {
            new JavaInstance(
                config, new VoidInputHandler(), Thread.currentThread().getContextClassLoader());
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
        JavaInstanceConfig config = createInstanceConfig();
        config.setTimeBudgetInMs(2000);
        JavaInstance instance = new JavaInstance(
            config, new VoidOutputHandler(), Thread.currentThread().getContextClassLoader());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage("1", "r", serialize(testString));
        assertNull(result.getUserException());
        assertNull(result.getTimeoutException());
        assertNull(result.getResult());
    }

    /**
     * Verify that function type should be consistent with serde type.
     */
    @Test
    public void testInconsistentType() {
        JavaInstanceConfig config = createInstanceConfig();
        config.setTimeBudgetInMs(2000);
        config.getFunctionConfig().setSerdeClassName(JavaSerDe.class.getName());

        try {
            new JavaInstance(
                config,
                (RequestHandler<String, String>) (input, context) -> input + "-lambda",
                Thread.currentThread().getContextClassLoader());
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException re) {
            assertTrue(re.getMessage().startsWith("Inconsistent types found between function class and serde class:"));
        }
    }
}
