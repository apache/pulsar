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
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SubscriptionCursorStatsAuthorizationProviderTest extends MockedPulsarStandalone {
    private static final String POLICY_HEADER = "X-Policy-Id";
    private static final String POLICY_ID = "cursor-diagnostics";
    private static final String DENIAL_REASON = "Cursor diagnostics are disabled by policy";

    public static class ForbiddenStatsProvider extends PulsarAuthorizationProvider {
        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role,
                TopicOperation operation, AuthenticationDataSource authData) {
            if (operation == TopicOperation.GET_STATS && NOBODY_SUBJECT.equals(role)) {
                Response response = Response.status(Response.Status.FORBIDDEN).type(MediaType.TEXT_PLAIN)
                        .header(POLICY_HEADER, POLICY_ID).entity(DENIAL_REASON).build();
                return CompletableFuture.failedFuture(new ForbiddenException(response));
            }
            return super.allowTopicOperationAsync(topic, role, operation, authData);
        }
    }

    @BeforeClass(alwaysRun = true)
    public void setup() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        getServiceConfiguration().setAuthorizationProvider(ForbiddenStatsProvider.class.getName());
        start();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        close();
    }

    @DataProvider
    public Object[][] cursorStatsPaths() {
        return new Object[][] {
                {"topic-partition-0/subscription/sub/internalStats"},
                {"topic/subscription/sub/partitioned-internalStats"}
        };
    }

    @Test(groups = "broker-admin", dataProvider = "cursorStatsPaths")
    public void testPreservesProviderForbiddenResponse(String path) throws Exception {
        URI uri = URI.create(getPulsarService().getWebServiceAddress() + "/admin/v2/persistent/public/default/" + path);
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + NOBODY_TOKEN).GET().build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(403);
            assertThat(response.headers().firstValue(HttpHeaders.CONTENT_TYPE)).hasValue(MediaType.TEXT_PLAIN);
            assertThat(response.headers().firstValue(POLICY_HEADER)).hasValue(POLICY_ID);
            assertThat(response.body()).isEqualTo(DENIAL_REASON);
        }
    }
}
