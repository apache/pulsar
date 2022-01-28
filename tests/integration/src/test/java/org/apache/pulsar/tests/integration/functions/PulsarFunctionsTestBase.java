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
import org.testng.annotations.DataProvider;

/**
 * A cluster to run pulsar functions for testing functions related features.
 */
@Slf4j
public abstract class PulsarFunctionsTestBase extends PulsarTestSuite {

    //
    // Common Variables used by functions test
    //
    public static final String EXCLAMATION_JAVA_CLASS =
        "org.apache.pulsar.functions.api.examples.ExclamationFunction";

    public static final String PUBLISH_JAVA_CLASS =
            "org.apache.pulsar.functions.api.examples.TypedMessageBuilderPublish";

    public static final String EXCEPTION_JAVA_CLASS =
            "org.apache.pulsar.tests.integration.functions.ExceptionFunction";

    public static final String SERDE_JAVA_CLASS =
            "org.apache.pulsar.functions.api.examples.CustomBaseToBaseFunction";

    public static final String SERDE_OUTPUT_CLASS =
            "org.apache.pulsar.functions.api.examples.CustomBaseSerde";

    public static final String EXCLAMATION_PYTHON_CLASS =
        "exclamation_function.ExclamationFunction";

    public static final String EXCLAMATION_WITH_DEPS_PYTHON_CLASS =
        "exclamation_with_extra_deps.ExclamationFunction";

    public static final String EXCLAMATION_PYTHON_ZIP_CLASS =
            "exclamation";

    public static final String PUBLISH_PYTHON_CLASS = "typed_message_builder_publish.TypedMessageBuilderPublish";
    public static final String EXCEPTION_PYTHON_CLASS = "exception_function";
    public static final String EXCLAMATION_PYTHON_FILE = "exclamation_function.py";
    public static final String EXCLAMATION_WITH_DEPS_PYTHON_FILE = "exclamation_with_extra_deps.py";
    public static final String EXCLAMATION_PYTHON_ZIP_FILE = "exclamation.zip";
    public static final String PUBLISH_FUNCTION_PYTHON_FILE = "typed_message_builder_publish.py";
    public static final String EXCEPTION_FUNCTION_PYTHON_FILE = "exception_function.py";

    public static final String EXCLAMATION_GO_FILE = "exclamationFunc";
    public static final String PUBLISH_FUNCTION_GO_FILE = "exclamationFunc";

    public static final String LOGGING_JAVA_CLASS =
            "org.apache.pulsar.functions.api.examples.LoggingFunction";

    @DataProvider(name = "FunctionRuntimeTypes")
    public static Object[][] getData() {
        return new Object[][] {
            { FunctionRuntimeType.PROCESS },
            { FunctionRuntimeType.THREAD }
        };
    }

    @DataProvider(name = "FunctionRuntimes")
    public static Object[][] functionRuntimes() {
        return new Object[][] {
            new Object[] { Runtime.JAVA },
            new Object[] { Runtime.PYTHON },
            new Object[] { Runtime.GO }
        };
    }

    protected final FunctionRuntimeType functionRuntimeType;

    public PulsarFunctionsTestBase() {
        this(FunctionRuntimeType.PROCESS);
    }

    protected PulsarFunctionsTestBase(FunctionRuntimeType functionRuntimeType) {
        this.functionRuntimeType = functionRuntimeType;
    }

    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
        setupFunctionWorkers();
    }

    @Override
    public void tearDownCluster() throws Exception {
        try {
            teardownFunctionWorkers();
        } finally {
            super.tearDownCluster();
        }
    }

    protected void setupFunctionWorkers() {
        final int numFunctionWorkers = 2;
        log.info("Setting up {} function workers : function runtime type = {}",
            numFunctionWorkers, functionRuntimeType);
        pulsarCluster.setupFunctionWorkers(randomName(5), functionRuntimeType, numFunctionWorkers);
        log.info("{} function workers has started", numFunctionWorkers);
    }

    protected void teardownFunctionWorkers() {
        log.info("Tearing down function workers ...");
        pulsarCluster.stopWorkers();
        log.info("All functions workers are stopped.");
    }

    protected static String getExclamationClass(Runtime runtime,
                                                boolean pyZip,
                                                boolean extraDeps) {
        if (Runtime.JAVA == runtime) {
            return EXCLAMATION_JAVA_CLASS;
        } else if (Runtime.PYTHON == runtime) {
            if (pyZip) {
                return EXCLAMATION_PYTHON_ZIP_CLASS;
            } else if (extraDeps) {
                return EXCLAMATION_WITH_DEPS_PYTHON_CLASS;
            } else {
                return EXCLAMATION_PYTHON_CLASS;
            }
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
    }
}
