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
package org.apache.pulsar.sql.presto;

import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.prestosql.spi.StandardErrorCode.QUERY_REJECTED;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import javax.crypto.SecretKey;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPulsarAuth extends MockedPulsarServiceBaseTest {
    private SecretKey secretKey;
    private final String SUPER_USER_ROLE = "admin";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.authentication.AuthenticationProviderToken"));
        conf.setAuthorizationEnabled(true);
        secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        conf.setProperties(properties);
        conf.setSuperUserRoles(Sets.newHashSet(SUPER_USER_ROLE));
        conf.setClusterName("c1");
        internalSetup();

        admin.clusters().createCluster("c1", ClusterData.builder().build());
        admin.tenants().createTenant("p1", new TenantInfoImpl(Sets.newHashSet(SUPER_USER_ROLE), Sets.newHashSet("c1")));
        waitForChange();
        admin.namespaces().createNamespace("p1/c1/ns1");
        waitForChange();
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        pulsarAdminBuilder.authentication(
                AuthenticationFactory.token(AuthTokenUtils.createToken(secretKey, SUPER_USER_ROLE, Optional.empty())));
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConfigCheck() {
        PulsarConnectorConfig pulsarConnectorConfig = new PulsarConnectorConfig();
        pulsarConnectorConfig.setAuthorizationEnabled(true);
        pulsarConnectorConfig.setBrokerBinaryServiceUrl("");

        new PulsarAuth(pulsarConnectorConfig);
    }

    @Test
    public void testEmptyExtraCredentials() {
        PulsarConnectorConfig pulsarConnectorConfig = mock(PulsarConnectorConfig.class);

        doReturn(true).when(pulsarConnectorConfig).getAuthorizationEnabled();
        doReturn(pulsar.getBrokerServiceUrl()).when(pulsarConnectorConfig).getBrokerBinaryServiceUrl();

        PulsarAuth pulsarAuth = new PulsarAuth(pulsarConnectorConfig);

        ConnectorSession session = mock(ConnectorSession.class);
        ConnectorIdentity identity = mock(ConnectorIdentity.class);
        doReturn("query-1").when(session).getQueryId();
        doReturn(identity).when(session).getIdentity();

        // Test empty extra credentials map
        doReturn(new HashMap<String, String>()).when(identity).getExtraCredentials();
        try {
            pulsarAuth.checkTopicAuth(session, "test");
            Assert.fail(); // should fail
        } catch (PrestoException e) {
            Assert.assertEquals(QUERY_REJECTED.toErrorCode(), e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("The credential information is empty"));
        }

        // Test empty extra credentials parameters
        doReturn(new HashMap<String, String>() {{
            put("auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        }}).when(identity).getExtraCredentials();
        try {
            pulsarAuth.checkTopicAuth(session, "test");
            Assert.fail(); // should fail
        } catch (PrestoException e) {
            Assert.assertEquals(QUERY_REJECTED.toErrorCode(), e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("Please specify the auth-method and auth-params"));
        }

        doReturn(new HashMap<String, String>() {{
            put("auth-params", "test-token");
        }}).when(identity).getExtraCredentials();
        try {
            pulsarAuth.checkTopicAuth(session, "test");
            Assert.fail(); // should fail
        } catch (PrestoException e) {
            Assert.assertEquals(QUERY_REJECTED.toErrorCode(), e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("Please specify the auth-method and auth-params"));
        }
    }

    @Test
    public void testPulsarSqlAuth() throws PulsarAdminException {
        String passRole = RandomStringUtils.randomAlphabetic(4) + "-pass";
        String deniedRole = RandomStringUtils.randomAlphabetic(4) + "-denied";
        String topic = "persistent://p1/c1/ns1/" + RandomStringUtils.randomAlphabetic(4);
        String otherTopic = "persistent://p1/c1/ns1/" + RandomStringUtils.randomAlphabetic(4) + "-other";
        String partitionedTopic = "persistent://p1/c1/ns1/" + RandomStringUtils.randomAlphabetic(4);
        String passToken = AuthTokenUtils.createToken(secretKey, passRole, Optional.empty());
        String deniedToken = AuthTokenUtils.createToken(secretKey, deniedRole, Optional.empty());

        admin.topics().grantPermission(topic, passRole, EnumSet.of(AuthAction.consume));
        admin.topics().createPartitionedTopic(partitionedTopic, 2);
        admin.topics().grantPermission(partitionedTopic, passRole, EnumSet.of(AuthAction.consume));
        waitForChange();

        ConnectorSession session = mock(ConnectorSession.class);
        ConnectorIdentity identity = mock(ConnectorIdentity.class);
        PulsarConnectorConfig pulsarConnectorConfig = mock(PulsarConnectorConfig.class);

        doReturn(true).when(pulsarConnectorConfig).getAuthorizationEnabled();
        doReturn(pulsar.getBrokerServiceUrl()).when(pulsarConnectorConfig).getBrokerBinaryServiceUrl();

        doReturn("query-1").when(session).getQueryId();
        doReturn(identity).when(session).getIdentity();

        doReturn(new HashMap<String, String>() {{
            put("auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
            put("auth-params", passToken);
        }}).when(identity).getExtraCredentials();

        PulsarAuth pulsarAuth = new PulsarAuth(pulsarConnectorConfig);

        pulsarAuth.checkTopicAuth(session, topic); // should pass

        // authorizedQueryTopicPairs should contain the authorized query and topic.
        Assert.assertTrue(
                pulsarAuth.authorizedQueryTopicsMap.containsKey(session.getQueryId()));
        Assert.assertTrue(pulsarAuth.authorizedQueryTopicsMap.get(session.getQueryId()).contains(topic));

        // Using the authorized query but not authorized topic should fail.
        // This part of the test case is for the case where a query accesses multiple topics but only some of them
        // have permission.
        try {
            pulsarAuth.checkTopicAuth(session, otherTopic);
            Assert.fail(); // should fail
        } catch (PrestoException e){
            Assert.assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("not authorized"));
        }

        // test clean session
        pulsarAuth.cleanSession(session);

        Assert.assertFalse(pulsarAuth.authorizedQueryTopicsMap.containsKey(session.getQueryId()));

        doReturn("test-fail").when(session).getQueryId();

        doReturn("query-2").when(session).getQueryId();

        try{
            doReturn(new HashMap<String, String>() {{
                put("auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                put("auth-params", "invalid-token");
            }}).when(identity).getExtraCredentials();
            pulsarAuth.checkTopicAuth(session, topic);
            Assert.fail(); // should fail
        } catch (PrestoException e){
            Assert.assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("Unable to authenticate"));
        }

        pulsarAuth.cleanSession(session);
        Assert.assertTrue(pulsarAuth.authorizedQueryTopicsMap.isEmpty());

        doReturn("query-3").when(session).getQueryId();

        try{
            doReturn(new HashMap<String, String>() {{
                put("auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
                put("auth-params", deniedToken);
            }}).when(identity).getExtraCredentials();
            pulsarAuth.checkTopicAuth(session, topic);
            Assert.fail(); // should fail
        } catch (PrestoException e){
            Assert.assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("not authorized"));
        }

        pulsarAuth.cleanSession(session);

        doReturn(new HashMap<String, String>() {{
            put("auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
            put("auth-params", passToken);
        }}).when(identity).getExtraCredentials();
        pulsarAuth.checkTopicAuth(session, topic); // should pass for the partitioned topic case

        pulsarAuth.cleanSession(session);
        Assert.assertTrue(pulsarAuth.authorizedQueryTopicsMap.isEmpty());
    }

    private static void waitForChange() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }
}
