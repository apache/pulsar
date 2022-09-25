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
package org.apache.pulsar.tests.integration.presto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.jsonwebtoken.SignatureAlgorithm;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.crypto.SecretKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.PrestoWorkerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class TestPulsarSQLAuth extends TestPulsarSQLBase {
    private SecretKey secretKey;
    private String adminToken;
    private PulsarAdmin admin;

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
                                                                            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder = super.beforeSetupCluster(clusterName, specBuilder);
        specBuilder.enablePrestoWorker(true);
        return specBuilder;
    }

    @Override
    protected void beforeStartCluster() {
        secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        adminToken = AuthTokenUtils.createToken(secretKey, "admin", Optional.empty());

        Map<String, String> envMap = new HashMap<>();
        envMap.put("authenticationEnabled", "true");
        envMap.put("authenticationProviders", "org.apache.pulsar.broker.authentication.AuthenticationProviderToken");
        envMap.put("authorizationEnabled", "true");
        envMap.put("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        envMap.put("superUserRoles", "admin");
        envMap.put("brokerDeleteInactiveTopicsEnabled", "false");

        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            brokerContainer.withEnv(envMap);
        }

        PrestoWorkerContainer prestoWorkerContainer = pulsarCluster.getPrestoWorkerContainer();

        prestoWorkerContainer
                .withEnv("SQL_PREFIX_pulsar.auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken")
                .withEnv("SQL_PREFIX_pulsar.auth-params", adminToken)
                .withEnv("pulsar.broker-binary-service-url", "pulsar://pulsar-broker-0:6650")
                .withEnv("pulsar.authorization-enabled", "true");

    }

    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
        initJdbcConnection();
        admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", adminToken)
                .build();
    }

    @Override
    public void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }

    @Test
    public void testPulsarSQLAuthCheck() throws PulsarAdminException {
        String passRole = RandomStringUtils.randomAlphabetic(4) + "-pass";
        String deniedRole = RandomStringUtils.randomAlphabetic(4) + "-denied";
        String passToken = AuthTokenUtils.createToken(secretKey, passRole, Optional.empty());
        String deniedToken = AuthTokenUtils.createToken(secretKey, deniedRole, Optional.empty());
        String topic = "testPulsarSQLAuthCheck";

        admin.topics().grantPermission(topic, passRole, EnumSet.of(AuthAction.consume));

        admin.topics().createNonPartitionedTopic(topic);

        String queryAllDataSql = String.format("select * from pulsar.\"%s\".\"%s\";", "public/default", topic);

        assertSQLExecution(
                () -> {
                    try {
                        ContainerExecResult containerExecResult =
                                execQuery(queryAllDataSql, new HashMap<>() {{
                                    put("auth-plugin",
                                            "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                                    put("auth-params", passToken);
                                }});
                        assertEquals(containerExecResult.getExitCode(), 0);
                    } catch (ContainerExecException e) {
                        fail(String.format("assertSQLExecution fail: %s", e.getLocalizedMessage()));
                    }
                }
        );

        assertSQLExecution(
                () -> {
                    try {
                        execQuery(queryAllDataSql, new HashMap<>() {{
                            put("auth-plugin",
                                    "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                            put("auth-params", "invalid-token");
                        }});
                        fail("Should not pass");
                    } catch (ContainerExecException e) {
                        // Authorization error
                        assertEquals(e.getResult().getExitCode(), 1);
                        log.info(e.getResult().getStderr());
                        assertTrue(e.getResult().getStderr().contains("Unable to authenticate"));
                    }
                }
        );

        assertSQLExecution(
                () -> {
                    try {
                        execQuery(queryAllDataSql, new HashMap<>() {{
                            put("auth-plugin",
                                    "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                            put("auth-params", deniedToken);
                        }});
                        fail("Should not pass");
                    } catch (ContainerExecException e) {
                        // Authorization error
                        assertEquals(e.getResult().getExitCode(), 1);
                        log.info(e.getResult().getStderr());
                        assertTrue(e.getResult().getStderr().contains("not authorized"));
                    }
                }
        );
    }

    @Test
    public void testCheckAuthForMultipleTopics() throws PulsarAdminException {
        String testRole = RandomStringUtils.randomAlphabetic(4) + "-test";
        String testToken = AuthTokenUtils.createToken(secretKey, testRole, Optional.empty());
        String topic1 = "testCheckAuthForMultipleTopics1";
        String topic2 = "testCheckAuthForMultipleTopics2";

        admin.topics().grantPermission(topic1, testRole, EnumSet.of(AuthAction.consume));

        admin.topics().createNonPartitionedTopic(topic1);

        admin.topics().createPartitionedTopic(topic2, 2); // Test for partitioned topic

        String queryAllDataSql =
                String.format("select * from pulsar.\"public/default\".\"%s\", pulsar.\"public/default\".\"%s\";",
                        topic1, topic2);

        assertSQLExecution(
                () -> {
                    try {
                        execQuery(queryAllDataSql, new HashMap<>() {{
                            put("auth-plugin",
                                    "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                            put("auth-params", testToken);
                        }});
                        fail("Should not pass");
                    } catch (ContainerExecException e) {
                        // Authorization error
                        assertEquals(e.getResult().getExitCode(), 1);
                        log.info(e.getResult().getStderr());
                    }
                }
        );

        admin.topics().grantPermission(topic2, testRole, EnumSet.of(AuthAction.consume));

        assertSQLExecution(
                () -> {
                    try {
                        ContainerExecResult containerExecResult =
                                execQuery(queryAllDataSql, new HashMap<>() {{
                                    put("auth-plugin",
                                            "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                                    put("auth-params", testToken);
                                }});

                        assertEquals(containerExecResult.getExitCode(), 0);
                    } catch (ContainerExecException e) {
                        fail(String.format("assertSQLExecution fail: %s", e.getLocalizedMessage()));
                    }
                }
        );
    }

    private void assertSQLExecution(org.awaitility.core.ThrowingRunnable assertion) {
        Awaitility.await()
                .pollDelay(Duration.ofMillis(0))
                .pollInterval(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(15))
                .untilAsserted(assertion);
    }
}
