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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.testng.annotations.Test;

import java.util.HashMap;

public class JavaInstanceTest {

    private static InstanceConfig createInstanceConfig() {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.addInputs("TEST");
        functionDetailsBuilder.setOutputSerdeClassName(DefaultSerDe.class.getName());
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
        return instanceConfig;
    }

    /**
     * Verify that be able to run lambda functions.
     * @throws Exception
     */
    @Test
    public void testLambda() {
        InstanceConfig config = createInstanceConfig();
        JavaInstance instance = new JavaInstance(
            config,
            (Function<String, String>) (input, context) -> input + "-lambda",
            null, null, new HashMap<>());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(MessageId.earliest, "random", testString);
        assertNotNull(result.getResult());
        assertEquals(new String(testString + "-lambda"), result.getResult());
    }
}
