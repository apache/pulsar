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

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.runtime.container.JavaSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.*;

public class JavaInstanceTest {

    private static final Logger log = LoggerFactory.getLogger(JavaInstanceTest.class);

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

    private byte[] serialize(Object resultValue) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(resultValue);
            out.flush();
            return bos.toByteArray();
        } catch (Exception ex) {
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return null;
    }

    private class UnSupportedClass {
        private String name;
        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    private class UnsupportedHandler implements RequestHandler<String, UnSupportedClass> {
        @Override
        public UnSupportedClass handleRequest(String input, Context context) throws Exception {
            return new UnSupportedClass();
        }
    }

    /**
     * Verify that functions running longer than time budget fails with Timeout exception
     * @throws Exception
     */
    @Test
    public void testLongRunningFunction() throws Exception {
        JavaInstanceConfig config = new JavaInstanceConfig();
        config.setTimeBudgetInMs(2000);
        config.setSerDe(new JavaSerDe());
        JavaInstance instance = new JavaInstance(config, new LongRunningHandler());
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
        JavaInstanceConfig config = new JavaInstanceConfig();
        config.setTimeBudgetInMs(2000);
        config.setSerDe(new JavaSerDe());
        JavaInstance instance = new JavaInstance(
            config,
            (RequestHandler<String, String>) (input, context) -> input + "-lambda");
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
        JavaInstanceConfig config = new JavaInstanceConfig();
        try {
            JavaInstance instance = new JavaInstance(config, new UnsupportedHandler());
            assertFalse(true);
        } catch (RuntimeException ex) {
            // Good
        } catch (Exception ex) {
            assertFalse(true);
        }
    }
}
