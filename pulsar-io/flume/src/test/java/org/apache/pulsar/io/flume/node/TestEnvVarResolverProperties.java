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
package org.apache.pulsar.io.flume.node;

import static org.testng.Assert.assertEquals;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import java.io.File;
import org.junit.Before;
import org.junit.Test;

public final class TestEnvVarResolverProperties {
    private static final File TEST_FILE = new File(
            TestEnvVarResolverProperties.class.getClassLoader()
                    .getResource("flume-conf-with-envvars.properties").getFile());

    private PropertiesFileConfigurationProvider provider;

    @Before
    public void setUp() {
        provider = new PropertiesFileConfigurationProvider("a1", TEST_FILE);
    }

    @Test
    public void resolveEnvVar() throws Exception {
        SystemLambda.withEnvironmentVariable("VARNAME", "varvalue").execute(() -> {
            String resolved = EnvVarResolverProperties.resolveEnvVars("padding ${VARNAME} padding");
            assertEquals(resolved, "padding varvalue padding");
        });
    }

    @Test
    public void resolveEnvVars() throws Exception {
        SystemLambda.withEnvironmentVariable("VARNAME1", "varvalue1")
                .and("VARNAME2", "varvalue2")
                .execute(() -> {
                    String resolved = EnvVarResolverProperties.resolveEnvVars(
                            "padding ${VARNAME1} ${VARNAME2} padding");
                    assertEquals(resolved, "padding varvalue1 varvalue2 padding");
                });
    }

    @Test
    public void getProperty() throws Exception {
        SystemLambda.withEnvironmentVariable("NC_PORT", "6667").execute(() -> {
            System.setProperty("propertiesImplementation",
                    "org.apache.pulsar.io.flume.node.EnvVarResolverProperties");

            assertEquals(provider.getFlumeConfiguration()
                    .getConfigurationFor("a1")
                    .getSourceContext().get("r1").getParameters().get("port"), "6667");
        });
    }
}