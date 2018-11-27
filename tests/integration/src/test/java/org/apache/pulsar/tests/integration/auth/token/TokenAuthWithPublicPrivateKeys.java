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

import java.nio.file.Files;
import java.nio.file.Path;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.shade.org.glassfish.jersey.internal.util.Base64;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

@Slf4j
public class TokenAuthWithPublicPrivateKeys extends PulsarTokenAuthenticationBaseSuite {

    private static final String PRIVATE_KEY_PATH_INSIDE_CONTAINER = "/tmp/private.key";
    private static final String PUBLIC_KEY_PATH_INSIDE_CONTAINER = "/tmp/public.key";

    private String publicKey;
    private String privateKey;

    @Override
    @SuppressWarnings("rawtypes")
    protected void createKeysAndTokens(PulsarContainer container) throws Exception {
        container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create-key-pair",
                        "--output-private-key", PRIVATE_KEY_PATH_INSIDE_CONTAINER,
                        "--output-public-key", PUBLIC_KEY_PATH_INSIDE_CONTAINER);

        Path privateKeyPath = Files.createTempFile("pulsar-private", "key");
        Path publicKeyPath = Files.createTempFile("pulsar-public", "key");
        container.copyFileFromContainer(PRIVATE_KEY_PATH_INSIDE_CONTAINER, privateKeyPath.toString());
        container.copyFileFromContainer(PUBLIC_KEY_PATH_INSIDE_CONTAINER, publicKeyPath.toString());

        privateKey = Base64.encodeAsString(Files.readAllBytes(privateKeyPath));
        publicKey = Base64.encodeAsString(Files.readAllBytes(publicKeyPath));

        clientAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--private-key", "data:base64," + privateKey,
                        "--subject", "regular-client")
                .getStdout();
        log.info("Created token: {}", clientAuthToken);

        superUserAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:base64," + privateKey,
                        "--subject", "super-user")
                .getStdout();
        log.info("Created token: {}", clientAuthToken);

    }

    @Override
    protected void configureBroker(BrokerContainer brokerContainer) throws Exception {
        brokerContainer.withEnv("PUBLIC_KEY", publicKey);
        brokerContainer.withEnv("tokenPublicKey", "env:PUBLIC_KEY");
    }

}
