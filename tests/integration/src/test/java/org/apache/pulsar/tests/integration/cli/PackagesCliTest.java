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

import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
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
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class PackagesCliTest extends TestRetrySupport {

    private static final String clusterNamePrefix = "packages-service";
    private PulsarCluster pulsarCluster;

    @BeforeClass(alwaysRun = true)
    public final void setup() throws Exception {
        incrementSetupNumber();
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .clusterName(String.format("%s-%s", clusterNamePrefix, RandomStringUtils.randomAlphabetic(6)))
                .brokerEnvs(getPackagesManagementServiceEnvs())
                .build();
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
    }

    @AfterClass(alwaysRun = true)
    public final void cleanup() {
        markCurrentSetupNumberCleaned();
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
    }

    private Map<String, String> getPackagesManagementServiceEnvs() {
        Map<String, String> envs = new HashMap<>();
        envs.put("enablePackagesManagement", "true");
        envs.put("packagesManagementLedgerRootPath", "/ledgers");
        return envs;
    }

    @Test(timeOut = 60000 * 5)
    public void testPackagesOperationsWithoutUploadingPackages() throws Exception {
        ContainerExecResult result = runPackagesCommand("list", "--type", "function", "public/default");
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("list-versions", "function://public/default/test");
        assertEquals(result.getExitCode(), 0);

        try {
            result = runPackagesCommand("download", "function://public/default/test@v1", "--path", "test-admin");
            fail("this command should be failed");
        } catch (Exception e) {
            // expected exception
        }
    }

    @Test(timeOut = 60000 * 8)
    public void testPackagesOperationsWithUploadingPackages() throws Exception {
        String testPackageName = "function://public/default/test@v1";
        ContainerExecResult result = runPackagesCommand("upload", "--description", "a test package",
                "--path", PulsarCluster.ADMIN_SCRIPT, testPackageName);
        assertEquals(result.getExitCode(), 0);

        BrokerContainer container = pulsarCluster.getBroker(0);
        String downloadFile = "tmp-file-" + RandomStringUtils.randomAlphabetic(8);
        String[] downloadCmd = new String[]{PulsarCluster.ADMIN_SCRIPT, "packages", "download",
                "--path", downloadFile, testPackageName};
        result = container.execCmd(downloadCmd);
        assertEquals(result.getExitCode(), 0);

        String[] diffCmd = new String[]{"diff", PulsarCluster.ADMIN_SCRIPT, downloadFile};
        result = container.execCmd(diffCmd);
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("get-metadata", testPackageName);
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("a test package"));

        result = runPackagesCommand("list", "--type", "function", "public/default");
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("test"));

        result = runPackagesCommand("list-versions", "function://public/default/test");
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("v1"));

        String contact = "test@apache.org";
        result = runPackagesCommand("update-metadata", "--description", "a test package",
                "--contact", contact, "-PpropertyA=A", testPackageName);
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("get-metadata", testPackageName);
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().isEmpty());
        assertTrue(result.getStdout().contains("a test package"));
        assertTrue(result.getStdout().contains(contact));
        assertTrue(result.getStdout().contains("propertyA"));

        result = runPackagesCommand("delete", testPackageName);
        assertEquals(result.getExitCode(), 0);

        result = runPackagesCommand("list-versions", "function://public/default/test");
        assertEquals(result.getExitCode(), 0);
        result.assertNoStdout();
    }

    private ContainerExecResult runPackagesCommand(String... commands) throws Exception {
        String[] cmds = new String[commands.length + 1];
        cmds[0] = "packages";
        System.arraycopy(commands, 0, cmds, 1, commands.length);
        return pulsarCluster.runAdminCommandOnAnyBroker(cmds);
    }
}
