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
package org.apache.pulsar.broker.admin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.crypto.SecretKey;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TopicsAuthTest extends MockedPulsarServiceBaseTest {

    private final String testLocalCluster = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String testTopicName = "my-topic";

    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String ADMIN_TOKEN = Jwts.builder().setSubject("admin").signWith(SECRET_KEY).compact();
    private static final String PRODUCE_TOKEN = Jwts.builder().setSubject("producer").signWith(SECRET_KEY).compact();
    private static final String CONSUME_TOKEN = Jwts.builder().setSubject("consumer").signWith(SECRET_KEY).compact();

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        // enable auth&auth and use JWT at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + ADMIN_TOKEN);
        super.internalSetup();
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
                ? brokerUrl.toString() : brokerUrlTls.toString())
                .authentication(AuthenticationToken.class.getName(),
                        ADMIN_TOKEN);
        admin = Mockito.spy(pulsarAdminBuilder.build());
        admin.clusters().createCluster(testLocalCluster, new ClusterDataImpl());
        admin.tenants().createTenant(testTenant, new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace,
                Sets.newHashSet(testLocalCluster));
        admin.namespaces().grantPermissionOnNamespace(testTenant + "/" + testNamespace, "producer",
                EnumSet.of(AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(testTenant + "/" + testNamespace, "consumer",
                EnumSet.of(AuthAction.consume));
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "variations")
    public static Object[][] variations() {
        return new Object[][]{
                {CONSUME_TOKEN, 401},
                {PRODUCE_TOKEN, 200}
        };
    }

    @Test(dataProvider = "variations")
    public void testProduceToNonPartitionedTopic(String token, int status) throws Exception {
        innerTestProduce(testTopicName, true, false, token, status);
    }

    @Test(dataProvider = "variations")
    public void testProduceToPartitionedTopic(String token, int status) throws Exception {
        innerTestProduce(testTopicName, true, true, token, status);
    }

    @Test(dataProvider = "variations")
    public void testProduceOnNonPersistentNonPartitionedTopic(String token, int status) throws Exception {
        innerTestProduce(testTopicName, false, false, token, status);
    }

    @Test(dataProvider = "variations")
    public void testProduceOnNonPersistentPartitionedTopic(String token, int status) throws Exception {
        innerTestProduce(testTopicName, false, true, token, status);
    }

    private void innerTestProduce(String createTopicName, boolean isPersistent, boolean isPartition,
                                  String token, int status) throws Exception {
        String topicPrefix = null;
        if (isPersistent == true) {
            topicPrefix = "persistent";
        } else {
            topicPrefix = "non-persistent";
        }
        if (isPartition == true) {
            admin.topics().createPartitionedTopic(topicPrefix + "://" + testTenant + "/"
                    + testNamespace + "/" + createTopicName, 5);
        } else {
            admin.topics().createNonPartitionedTopic(topicPrefix + "://" + testTenant + "/"
                    + testNamespace + "/" + createTopicName);
        }
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {
                }));

        WebTarget root = buildWebClient();
        String requestPath = null;
        if (isPartition == true) {
            requestPath = "/topics/" + topicPrefix + "/" + testTenant + "/" + testNamespace + "/"
                    + createTopicName + "/partitions/2";
        } else {
            requestPath = "/topics/" + topicPrefix + "/" + testTenant + "/" + testNamespace + "/" + createTopicName;
        }

        Response response = root.path(requestPath)
                .request(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + token)
                .post(Entity.json(producerMessages));
        Assert.assertEquals(response.getStatus(), status);
    }

    WebTarget buildWebClient() throws Exception {
        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);

        javax.ws.rs.client.ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(httpConfig);
        Client client = clientBuilder.build();
        return client.target(brokerUrl.toString());
    }

}
