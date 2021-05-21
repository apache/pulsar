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
package org.apache.pulsar.tests.integration.auth.admin;

import com.google.common.io.Files;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.elasticsearch.common.collect.Set;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/**
 * PackagesOpsWithAuthTest will test all package operations with and without the proper permission.
 */
@Slf4j
public class PackagesOpsWithAuthTest extends TestRetrySupport {

    private static final String CLUSTER_PREFIX = "package-auth";
    private static final String PRIVATE_KEY_PATH_INSIDE_CONTAINER = "/tmp/private.key";
    private static final String PUBLIC_KEY_PATH_INSIDE_CONTAINER = "/tmp/public.key";

    private static final String SUPER_USER_ROLE = "super-user";
    private String superUserAuthToken;
    private static final String PROXY_ROLE = "proxy";
    private String proxyAuthToken;
    private static final String REGULAR_USER_ROLE = "client";
    private String clientAuthToken;
    private File publicKeyFile;

    private PulsarCluster pulsarCluster;
    private PulsarContainer cmdContainer;

    @Override
    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        incrementSetupNumber();
        // Before starting the cluster, generate the secret key and the token
        // Use Zk container to have 1 container available before starting the cluster
        final String clusterName = String.format("%s-%s", CLUSTER_PREFIX, RandomStringUtils.randomAlphabetic(6));
        final String cliContainerName = String.format("%s-%s", "cli", RandomStringUtils.randomAlphabetic(6));
        cmdContainer = new ZKContainer<>(cliContainerName);
        cmdContainer
            .withNetwork(Network.newNetwork())
            .withNetworkAliases(ZKContainer.NAME)
            .withEnv("zkServers", ZKContainer.NAME);
        cmdContainer.start();

        createKeysAndTokens(cmdContainer);

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .numBookies(2)
            .numBrokers(2)
            .numProxies(1)
            .clusterName(clusterName)
            .brokerEnvs(getBrokerSettingsEnvs())
            .proxyEnvs(getProxySettingsEnvs())
            .brokerMountFiles(Collections.singletonMap(publicKeyFile.toString(), PUBLIC_KEY_PATH_INSIDE_CONTAINER))
            .proxyMountFiles(Collections.singletonMap(publicKeyFile.toString(), PUBLIC_KEY_PATH_INSIDE_CONTAINER))
            .build();

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        markCurrentSetupNumberCleaned();
        if (cmdContainer != null) {
            cmdContainer.stop();
        }
        if (pulsarCluster != null) {
            pulsarCluster.stop();
        }
    }

    private Map<String, String> getBrokerSettingsEnvs() {
        Map<String, String> envs = new HashMap<>();
        envs.put("enablePackagesManagement", "true");
        envs.put("authenticationEnabled", "true");
        envs.put("authenticationProviders", AuthenticationProviderToken.class.getName());
        envs.put("authorizationEnabled", "true");
        envs.put("superUserRoles", String.format("%s,%s", SUPER_USER_ROLE, PROXY_ROLE));
        envs.put("brokerClientAuthenticationPlugin", AuthenticationToken.class.getName());
        envs.put("brokerClientAuthenticationParameters", String.format("token:%s", superUserAuthToken));
        envs.put("authenticationRefreshCheckSeconds", "1");
        envs.put("authenticateOriginalAuthData", "true");
        envs.put("tokenPublicKey", "file://" + PUBLIC_KEY_PATH_INSIDE_CONTAINER);
        return envs;
    }

    private Map<String, String> getProxySettingsEnvs() {
        Map<String, String> envs = new HashMap<>();
        envs.put("authenticationEnabled", "true");
        envs.put("authenticationProviders", AuthenticationProviderToken.class.getName());
        envs.put("authorizationEnabled", "true");
        envs.put("brokerClientAuthenticationPlugin", AuthenticationToken.class.getName());
        envs.put("brokerClientAuthenticationParameters", String.format("token:%s", proxyAuthToken));
        envs.put("authenticationRefreshCheckSeconds", "1");
        envs.put("forwardAuthorizationCredentials", "true");
        envs.put("tokenPublicKey", "file://" + PUBLIC_KEY_PATH_INSIDE_CONTAINER);
        return envs;
    }

    protected void createKeysAndTokens(PulsarContainer container) throws Exception {
        container
            .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create-key-pair",
                "--output-private-key", PRIVATE_KEY_PATH_INSIDE_CONTAINER,
                "--output-public-key", PUBLIC_KEY_PATH_INSIDE_CONTAINER);

        byte[] publicKeyBytes = DockerUtils
            .runCommandWithRawOutput(container.getDockerClient(), container.getContainerId(),
                "/bin/cat", PUBLIC_KEY_PATH_INSIDE_CONTAINER)
            .getStdout();

        publicKeyFile = File.createTempFile("public-", ".key", new File("/tmp"));
        Files.write(publicKeyBytes, publicKeyFile);

        clientAuthToken = container
            .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                "--private-key", "file://" + PRIVATE_KEY_PATH_INSIDE_CONTAINER,
                "--subject", REGULAR_USER_ROLE)
            .getStdout().trim();
        log.info("Created client token: {}", clientAuthToken);

        superUserAuthToken = container
            .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                "--private-key", "file://" + PRIVATE_KEY_PATH_INSIDE_CONTAINER,
                "--subject", SUPER_USER_ROLE)
            .getStdout().trim();
        log.info("Created super-user token: {}", superUserAuthToken);

        proxyAuthToken = container
            .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                "--private-key", "file://" + PRIVATE_KEY_PATH_INSIDE_CONTAINER,
                "--subject", PROXY_ROLE)
            .getStdout().trim();
        log.info("Created proxy token: {}", proxyAuthToken);
    }

    @Test
    public void testPackagesOps() throws Exception {
        @Cleanup
        PulsarAdmin superUserAdmin = PulsarAdmin.builder()
            .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
            .authentication(AuthenticationFactory.token(superUserAuthToken))
            .build();

        @Cleanup
        PulsarAdmin clientAdmin = PulsarAdmin.builder()
            .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
            .authentication(AuthenticationFactory.token(clientAuthToken))
            .build();

        // do some operation without grant any permissions
        try {
            List<String> packagesName = clientAdmin.packages().listPackages("function", "public/default");
            fail("list package operation should fail because the client hasn't permission to do");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 401);
        }

        // grant package permission to the role
        superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
            REGULAR_USER_ROLE, Set.of(AuthAction.packages));

        // then do some package operations again, it should success
        List<String> packagesName = clientAdmin.packages().listPackages("function", "public/default");
        assertEquals(packagesName.size(), 0);
    }
}
