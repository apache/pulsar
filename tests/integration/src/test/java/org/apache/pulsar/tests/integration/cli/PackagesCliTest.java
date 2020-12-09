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
package org.apache.pulsar.tests.integration.cli;

import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class PackagesCliTest {

    private final static String clusterNamePrefix = "packages-service";
    private PulsarCluster pulsarCluster;

    @BeforeClass
    public void setup() throws Exception {
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .clusterName(String.format("%s-%s", clusterNamePrefix, RandomStringUtils.randomAlphabetic(6)))
            .brokerEnvs(getPackagesManagementServiceEnvs())
            .build();
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
    }

    @AfterClass
    public void teardown() {
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
    }

    private Map<String, String> getPackagesManagementServiceEnvs() {
        Map<String, String> envs = new HashMap<>();
        envs.put("enablePackagesManagement", "true");
        return envs;
    }

    @Test
    public void testPackagesOperationsWithoutUploadingPackages() throws Exception {
        ContainerExecResult result = runPackagesCommand("list", "--type", "function", "public/default");
        assertEquals(result.getExitCode(), 0);
        assertTrue(result.getStdout().isEmpty());

        result = runPackagesCommand("list-versions", "function://public/default/test");
        assertEquals(result.getExitCode(), 0);
        assertTrue(result.getStdout().isEmpty());

        try {
            result = runPackagesCommand("download", "function://public/default/test@v1", "--path", "test-admin");
            fail("this command should be failed");
        } catch (Exception e) {
            // expected exception
        }
    }

    // TODO: the upload command has some problem when uploading packages, enable this tests when issue
    // https://github.com/apache/pulsar/issues/8874 is fixed.
    @Ignore
    @Test
    public void testPackagesOperationsWithUploadingPackages() throws Exception {
        String testPackageName = "function://public/default/test@v1";
        ContainerExecResult result = runPackagesCommand("upload", "--description", "a test package",
            "--path", PulsarCluster.ADMIN_SCRIPT, testPackageName);
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("get-metadata", testPackageName);
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("a test package"));

        result = runPackagesCommand("list", "--type", "function", "public/default");
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains(testPackageName));

        result = runPackagesCommand("list-versions", "--type", "function://public/default/test");
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("v1"));

        String contact = "test@apache.org";
        result = runPackagesCommand("update-metadata", "--contact", contact, "-PpropertyA=A", testPackageName);
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("get-metadata", testPackageName);
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("a test package"));
        assertTrue(result.getStdout().contains(contact));
        assertTrue(result.getStdout().contains("propertyA"));

        result = runPackagesCommand("delete", testPackageName);
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("list", "--type", "functions", "public/default");
        assertEquals(result.getExitCode(), 0);
        assertTrue(result.getStdout().isEmpty());
    }

    private ContainerExecResult runPackagesCommand(String... commands) throws Exception {
        String[] cmds = new String[commands.length + 1];
        cmds[0] = "packages";
        System.arraycopy(commands, 0, cmds, 1, commands.length);
        return pulsarCluster.runAdminCommandOnAnyBroker(cmds);
    }
}
