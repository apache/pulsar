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

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenAuthWithSymmetricKeys extends PulsarTokenAuthenticationBaseSuite {

    private String secretKey;

    @Override
    @SuppressWarnings("rawtypes")
    protected void createKeysAndTokens(PulsarContainer container) throws Exception {
        secretKey = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create-secret-key", "--base64")
                .getStdout();
        log.info("Created secret key: {}", secretKey);

        clientAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", REGULAR_USER_ROLE)
                .getStdout().trim();
        log.info("Created client token: {}", clientAuthToken);

        superUserAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", SUPER_USER_ROLE)
                .getStdout().trim();
        log.info("Created super-user token: {}", superUserAuthToken);

        proxyAuthToken = container
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", PROXY_ROLE)
                .getStdout().trim();
        log.info("Created proxy token: {}", proxyAuthToken);
    }

    @Override
    protected void configureBroker(BrokerContainer brokerContainer) throws Exception {
        brokerContainer.withEnv("tokenSecretKey", "data:;base64," + secretKey);
    }

    @Override
    protected void configureProxy(ProxyContainer proxyContainer) throws Exception {
        proxyContainer.withEnv("tokenSecretKey", "data:;base64," + secretKey);
    }

    @Override
    protected String createClientTokenWithExpiry(long expiryTime, TimeUnit unit) throws Exception {
        return cmdContainer
                .execCmd(PulsarCluster.PULSAR_COMMAND_SCRIPT, "tokens", "create",
                        "--secret-key", "data:;base64," + secretKey,
                        "--subject", REGULAR_USER_ROLE,
                        "--expiry-time", unit.toSeconds(expiryTime) + "s")
                .getStdout().trim();
    }
}
