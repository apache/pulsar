/*
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
package org.apache.pulsar.broker.stats;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AuthenticatedConsumerStatsTest extends ConsumerStatsTest{
    private final String ADMIN_TOKEN;
    private final String TOKEN_PUBLIC_KEY;
    private final KeyPair kp;

    AuthenticatedConsumerStatsTest() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kp = kpg.generateKeyPair();

        byte[] encodedPublicKey = kp.getPublic().getEncoded();
        TOKEN_PUBLIC_KEY = "data:;base64," + Base64.getEncoder().encodeToString(encodedPublicKey);
        ADMIN_TOKEN = generateToken(kp, "admin");
    }


    private String generateToken(KeyPair kp, String subject) {
        PrivateKey pkey = kp.getPrivate();
        long expMillis = System.currentTimeMillis() + Duration.ofHours(1).toMillis();
        Date exp = new Date(expMillis);

        return Jwts.builder()
                .setSubject(subject)
                .setExpiration(exp)
                .signWith(pkey, SignatureAlgorithm.forSigningKey(pkey))
                .compact();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.authentication(AuthenticationFactory.token(ADMIN_TOKEN));
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        pulsarAdminBuilder.authentication(AuthenticationFactory.token(ADMIN_TOKEN));
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + ADMIN_TOKEN);

        conf.setClusterName("test");

        // Set provider domain name
        Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", TOKEN_PUBLIC_KEY);
        conf.setProperties(properties);

        super.internalSetup();
        super.producerBaseSetup();
    }

    @Test
    public void testConsumerStatsOutput() throws Exception {
        final String topicName = "persistent://public/default/testConsumerStatsOutput";
        final String subName = "my-subscription";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topicName);
        ObjectMapper mapper = ObjectMapperFactory.create();
        ConsumerStats consumerStats = stats.getSubscriptions()
                .get(subName).getConsumers().get(0);
        // assert that role is exposed
        Assert.assertEquals(consumerStats.getAppId(), "admin");
        consumer.close();
    }

}
