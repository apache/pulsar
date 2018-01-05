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

import static org.testng.Assert.assertEquals;

import java.net.URL;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionConfig}.
 */
public class FunctionConfigTest {

    public static final String TEST_NAME = "test-function-config";

    @Test
    public void testGetterSetter() {
        FunctionConfig fc = new FunctionConfig();
        fc.setSinkTopic(TEST_NAME + "-sink");
        fc.setSourceTopic(TEST_NAME + "-source");
        fc.setName(TEST_NAME);

        assertEquals(TEST_NAME, fc.getName());
        assertEquals(TEST_NAME + "-sink", fc.getSinkTopic());
        assertEquals(TEST_NAME + "-source", fc.getSourceTopic());
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
