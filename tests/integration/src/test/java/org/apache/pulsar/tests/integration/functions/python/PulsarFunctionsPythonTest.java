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
package org.apache.pulsar.tests.integration.functions.python;

import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.testng.annotations.Test;

public class PulsarFunctionsPythonTest extends PulsarFunctionsTest {

	PulsarFunctionsPythonTest(FunctionRuntimeType functionRuntimeType) {
		super(functionRuntimeType);
	}

    @Test(groups = {"python_function", "function"})
    public void testPythonFunctionLocalRun() throws Exception {
        testFunctionLocalRun(Runtime.PYTHON);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonFunctionNegAck() throws Exception {
        testFunctionNegAck(Runtime.PYTHON);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonPublishFunction() throws Exception {
        testPublishFunction(Runtime.PYTHON);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, false, false);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationFunctionWithExtraDeps() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, false, true);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationZipFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, true, false);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationTopicPatternFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, true, false, false);
    }

}
