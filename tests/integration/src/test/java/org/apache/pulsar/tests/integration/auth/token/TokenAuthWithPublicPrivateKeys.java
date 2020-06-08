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
package org.apache.pulsar.tests.integration.auth.token;

import com.google.common.io.Files;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.utils.DockerUtils;

@Slf4j
public class TokenAuthWithPublicPrivateKeys extends PulsarTokenAuthenticationBaseSuite {

    private static final String PRIVATE_KEY_PATH_INSIDE_CONTAINER = "/tmp/private.key";
    private static final String PUBLIC_KEY_PATH_INSIDE_CONTAINER = "/tmp/public.key";

    private File publicKeyFile;

    @Override
    @SuppressWarnings("rawtypes")
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

    @Override
    protected void configureBroker(BrokerContainer brokerContainer) throws Exception {
        brokerContainer.withFileSystemBind(publicKeyFile.toString(), PUBLIC_KEY_PATH_INSIDE_CONTAINER);
        brokerContainer.withEnv("tokenPublicKey", "file://" + PUBLIC_KEY_PATH_INSIDE_CONTAINER);
    }

    @Override
    protected void configureProxy(ProxyContainer proxyContainer) throws Exception {
        proxyContainer.withFileSystemBind(publicKeyFile.toString(), PUBLIC_KEY_PATH_INSIDE_CONTAINER);
        proxyContainer.withEnv("tokenPublicKey", "file://" + PUBLIC_KEY_PATH_INSIDE_CONTAINER);
    }

    @Override
    protected String createClientTokenWithExpiry(long expiryTime, TimeUnit unit) throws Exception {
        return cmdContainer
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--private-key", "file://" + PRIVATE_KEY_PATH_INSIDE_CONTAINER,
                        "--subject", REGULAR_USER_ROLE,
                        "--expiry-time", unit.toSeconds(expiryTime) + "s")
                .getStdout().trim();
    }
}
