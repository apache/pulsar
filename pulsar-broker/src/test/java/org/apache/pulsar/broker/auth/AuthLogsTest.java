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
package org.apache.pulsar.broker.auth;

import static org.testng.Assert.fail;

import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterClass;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.ServerSideErrorException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.AuthenticationException;
import org.apache.pulsar.client.api.PulsarClientException.AuthorizationException;
import org.apache.pulsar.client.api.Producer;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test doesn't test much in and off itself.
 * However it is useful to see which logs are produced when there's an
 * failure or error in authentication.
 */
@Test(groups = "flaky")
public class AuthLogsTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(AuthLogsTest.class);

    public AuthLogsTest() {
        super();
    }

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setClusterName("test");
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.auth.MockAuthenticationProvider"));
        conf.setAuthorizationProvider("org.apache.pulsar.broker.auth.MockAuthorizationProvider");
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet("super"));
        internalSetup();

        try (PulsarAdmin admin = PulsarAdmin.builder()
             .authentication(new MockAuthentication("pass.pass"))
             .serviceHttpUrl(brokerUrl.toString()).build()) {
            admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
            admin.tenants().createTenant("public",
                                         new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        }
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void binaryEndpoint() throws Exception {
        log.info("LOG_TEST_SUCCESS_CLIENT should succeeed both client");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("pass.pass"))
                .build();
             Producer<byte[]> producer = client.newProducer().topic("foobar").create();
             Consumer<byte[]> consumer = client.newConsumer().topic("foobar")
                .subscriptionName("foobar").subscribe()) {
        }

        log.info("LOG_TEST_PRODUCER_AUTHN_FAIL");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("fail.ignored"))
                .build();
             Producer<byte[]> producer = client.newProducer().topic("foobar").create()) {
            fail("Should fail auth");
        } catch (AuthenticationException ae) { /* expected */ }

        log.info("LOG_TEST_PRODUCER_AUTHN_ERROR");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("error.ignored"))
                .build();
             Producer<byte[]> producer = client.newProducer().topic("foobar").create()) {
            fail("Should fail auth");
        } catch (AuthenticationException ae) { /* expected */ }

        log.info("LOG_TEST_CONSUMER_AUTHN_FAIL");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("fail.ignored"))
                .build();
             Consumer<byte[]> consumer = client.newConsumer().topic("foobar")
                .subscriptionName("foobar").subscribe()) {
            fail("Should fail auth");
        } catch (AuthenticationException ae) { /* expected */ }

        log.info("LOG_TEST_CONSUMER_AUTHN_ERROR");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("error.ignored"))
                .build();
             Consumer<byte[]> consumer = client.newConsumer().topic("foobar")
                .subscriptionName("foobar").subscribe()) {
            fail("Should fail auth");
        } catch (AuthenticationException ae) { /* expected */ }

        log.info("LOG_TEST_PRODUCER_AUTHZ_FAIL");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("pass.fail"))
                .build();
             Producer<byte[]> producer = client.newProducer().topic("foobar").create()) {
            fail("Should fail auth");
        } catch (AuthorizationException ae) { /* expected */ }

        log.info("LOG_TEST_PRODUCER_AUTHZ_ERROR");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("pass.error"))
                .build();
             Producer<byte[]> producer = client.newProducer().topic("foobar").create()) {
            fail("Should fail auth");
        } catch (AuthorizationException ae) { /* expected */ }

        log.info("LOG_TEST_CONSUMER_AUTHZ_FAIL");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("pass.fail"))
                .build();
             Consumer<byte[]> consumer = client.newConsumer().topic("foobar")
                .subscriptionName("foobar").subscribe()) {
            fail("Should fail auth");
        } catch (AuthorizationException ae) { /* expected */ }

        log.info("LOG_TEST_CONSUMER_AUTHZ_ERROR");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new MockAuthentication("pass.error"))
                .build();
             Consumer<byte[]> consumer = client.newConsumer().topic("foobar")
                .subscriptionName("foobar").subscribe()) {
            fail("Should fail auth");
        } catch (AuthorizationException ae) { /* expected */ }

        log.info("LOG_TEST_CLIENT_DONE");
    }

    @Test
    public void httpEndpoint() throws Exception {
        log.info("LOG_TEST_SUCCESS_CLIENT should succeeed both client");
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .authentication(new MockAuthentication("pass.pass"))
                .serviceHttpUrl(brokerUrl.toString()).build()) {
            admin.namespaces().getNamespaces("public");
        }

        log.info("LOG_TEST_HTTP_AUTHN_FAIL");
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .authentication(new MockAuthentication("fail.ignore"))
                .serviceHttpUrl(brokerUrl.toString()).build()) {
            admin.namespaces().getNamespaces("public");
            fail("Should fail auth");
        } catch (NotAuthorizedException ae) { /* expected */ }

        log.info("LOG_TEST_HTTP_AUTHN_ERROR");
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .authentication(new MockAuthentication("error.ignore"))
                .serviceHttpUrl(brokerUrl.toString()).build()) {
            admin.namespaces().getNamespaces("public");
            fail("Should fail auth");
        } catch (NotAuthorizedException ae) { /* expected */ }


        log.info("LOG_TEST_HTTP_AUTHZ_FAIL");
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .authentication(new MockAuthentication("pass.fail"))
                .serviceHttpUrl(brokerUrl.toString()).build()) {
            admin.namespaces().getNamespaces("public");
            fail("Should fail auth");
        } catch (NotAuthorizedException ae) { /* expected */ }

        log.info("LOG_TEST_HTTP_AUTHZ_ERROR");
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .authentication(new MockAuthentication("pass.error"))
                .serviceHttpUrl(brokerUrl.toString()).build()) {
            admin.namespaces().getNamespaces("public");
            fail("Should fail auth");
        } catch (ServerSideErrorException ae) { /* expected */ }


        log.info("LOG_TEST_CLIENT_DONE");
    }

}
