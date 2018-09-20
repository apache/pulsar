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
package org.apache.pulsar.tests.integration.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

/**
 * A cluster to run pulsar functions for testing functions related features.
 */
@Slf4j
public abstract class PulsarFunctionsTestBase extends PulsarTestSuite {

    @DataProvider(name = "FunctionRuntimeTypes")
    public static Object[][] getData() {
        return new Object[][] {
            { FunctionRuntimeType.PROCESS },
            { FunctionRuntimeType.THREAD }
        };
    }

    protected final FunctionRuntimeType functionRuntimeType;

    public PulsarFunctionsTestBase() {
        this(FunctionRuntimeType.PROCESS);
    }

    protected PulsarFunctionsTestBase(FunctionRuntimeType functionRuntimeType) {
        this.functionRuntimeType = functionRuntimeType;
    }

    @BeforeClass
    public void setupFunctionWorkers() {
        final int numFunctionWorkers = 3;
        log.info("Setting up {} function workers : function runtime type = {}",
            numFunctionWorkers, functionRuntimeType);
        pulsarCluster.setupFunctionWorkers(randomName(5), functionRuntimeType, numFunctionWorkers);
        log.info("{} function workers has started", numFunctionWorkers);
    }

    @AfterClass
    public void teardownFunctionWorkers() {
        log.info("Tearing down function workers ...");
        pulsarCluster.stopWorkers();
        log.info("All functions workers are stopped.");
    }

    //
    // Common Variables used by functions test
    //

    public static final String EXCLAMATION_JAVA_CLASS =
        "org.apache.pulsar.functions.api.examples.ExclamationFunction";

    public static final String EXCLAMATION_PYTHON_CLASS =
        "exclamation.ExclamationFunction";

    public static final String EXCLAMATION_PYTHON_FILE = "exclamation_function.py";

    protected static String getExclamationClass(Runtime runtime) {
        if (Runtime.JAVA == runtime) {
            return EXCLAMATION_JAVA_CLASS;
        } else if (Runtime.PYTHON == runtime) {
            return EXCLAMATION_PYTHON_CLASS;
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
    }

    @DataProvider(name = "FunctionRuntimes")
    public static Object[][] functionRuntimes() {
        return new Object[][] {
            new Object[] { Runtime.JAVA },
            new Object[] { Runtime.PYTHON }
        };
    }

}
