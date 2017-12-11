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

package org.apache.pulsar.functions.fs;

import static org.junit.Assert.assertEquals;

import java.net.URL;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionConfig}.
 */
public class FunctionConfigTest {

    @Rule
    public final TestName runtime = new TestName();

    @Test
    public void testGetterSetter() {
        FunctionConfig fc = new FunctionConfig();
        fc.setSinkTopic(runtime.getMethodName() + "-sink");
        fc.setSourceTopic(runtime.getMethodName() + "-source");
        fc.setName(runtime.getMethodName());

        assertEquals(runtime.getMethodName(), fc.getName());
        assertEquals(runtime.getMethodName() + "-sink", fc.getSinkTopic());
        assertEquals(runtime.getMethodName() + "-source", fc.getSourceTopic());
    }

    @Test
    public void testLoadFunctionConfig() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_function_config.yml");
        FunctionConfig fc = FunctionConfig.load(yamlUrl.toURI().getPath());

        assertEquals("test-function", fc.getName());
        assertEquals("test-sink-topic", fc.getSinkTopic());
        assertEquals("test-source-topic", fc.getSourceTopic());
    }

}
