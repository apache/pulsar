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
import org.apache.pulsar.tests.integration.functions.runtime.PulsarFunctionsRuntimeTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Container.ExecResult;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

/**
 * A cluster to run pulsar functions for testing functions related features.
 */
@Slf4j
public abstract class PulsarFunctionsTestBase extends PulsarClusterTestBase  {

    public static final String EXCLAMATION_JAVA_CLASS =
        "org.apache.pulsar.functions.api.examples.ExclamationFunction";

    public static final String EXCLAMATION_PYTHON_CLASS =
        "exclamation.Exclamation";

    public static final String EXCLAMATION_PYTHON_FILE = "exclamation.py";

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
