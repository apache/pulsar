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
package org.apache.pulsar.broker.admin;

import static org.assertj.core.api.Assertions.assertThat;
import io.jsonwebtoken.Jwts;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.MultiBrokerTestZKBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class SubscriptionCursorInternalStatsMultiBrokerTest extends MultiBrokerTestZKBaseTest {
    private static final SecretKey KEY = Jwts.SIG.HS256.key().build();
    private static final String BROKER_TOKEN = Jwts.builder().subject("broker").signWith(KEY).compact();
    private static final String READER_TOKEN = Jwts.builder().subject("reader").signWith(KEY).compact();

    @Override
    protected int numberOfAdditionalBrokers() {
        return 1;
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration config = super.getDefaultConf();
        config.setAuthenticationEnabled(true);
        config.setAuthorizationEnabled(true);
        config.setSuperUserRoles(Set.of("broker"));
        config.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        config.getProperties().setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(KEY));
        config.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        config.setBrokerClientAuthenticationParameters("token:" + BROKER_TOKEN);
        return config;
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder builder) {
        builder.authentication(new AuthenticationToken(BROKER_TOKEN));
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder builder) {
        builder.authentication(new AuthenticationToken(BROKER_TOKEN));
    }

    @Test
    public void testPartitionFanOutFollowsPhysicalRedirect() throws Exception {
        String topic = "persistent://public/default/cursor-stats";
        String subscription = "subscription";
        TopicName firstPartition = TopicName.get(topic).getPartition(0);
        TopicName secondPartition = TopicName.get(topic).getPartition(1);
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createSubscription(firstPartition.toString(), subscription, MessageId.earliest);
        admin.topics().createSubscription(secondPartition.toString(), subscription, MessageId.earliest);
        admin.namespaces().grantPermissionOnNamespace("public/default", "reader", Set.of(AuthAction.consume));

        String ownerUrl = admin.lookups().lookupTopic(firstPartition.toString());
        var brokers = getAllBrokers();
        PulsarService owner = brokers.stream().filter(broker -> broker.getBrokerServiceUrl().equals(ownerUrl))
                .findFirst().orElseThrow();
        PulsarService coordinator = brokers.stream().filter(broker -> broker != owner).findFirst().orElseThrow();
        String adminUrl = coordinator.getWebServiceAddress() + "/admin/v2/";
        URI uri = URI.create(adminUrl + firstPartition.getRestPath() + "/subscription/"
                + subscription + "/internalStats");
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + READER_TOKEN).GET().build();
        try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()) {
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            assertThat(response.statusCode()).isEqualTo(307);
            URI location = URI.create(response.headers().firstValue("Location").orElseThrow());
            assertThat(location.getAuthority()).isEqualTo(URI.create(owner.getWebServiceAddress()).getAuthority());

            URI partitionedUri = URI.create(adminUrl + TopicName.get(topic).getRestPath() + "/subscription/"
                    + subscription + "/partitioned-internalStats");
            HttpRequest partitionedRequest = HttpRequest.newBuilder(partitionedUri)
                    .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + READER_TOKEN).GET().build();
            HttpResponse<String> partitionedResponse = client.send(partitionedRequest,
                    HttpResponse.BodyHandlers.ofString());
            assertThat(partitionedResponse.statusCode()).as("response body: %s", partitionedResponse.body())
                    .isEqualTo(200);
            PartitionedSubscriptionCursorInternalStats result = ObjectMapperFactory.getMapper().reader()
                    .forType(PartitionedSubscriptionCursorInternalStats.class).readValue(partitionedResponse.body());
            assertThat(result.complete).isTrue();
            assertThat(result.partitions).containsOnlyKeys(firstPartition.toString(), secondPartition.toString());
            assertThat(result.errors).isEmpty();
        }
    }
}
