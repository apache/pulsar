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
package org.apache.pulsar.client.admin.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.GettingAuthenticationDataException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.internal.BaseResource.FutureCallback;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException.AuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BaseResourceTest {

    WireMockServer server;
    String adminUrl;

    @BeforeClass(alwaysRun = true)
    void beforeClass() {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        server.start();
        adminUrl = "http://localhost:" + server.port();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    private String httpErrorFor(String path, ResponseDefinitionBuilder response) throws Exception {
        server.stubFor(get(urlEqualTo(path)).willReturn(response));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
        NamespacesImpl namespaces = (NamespacesImpl) admin.namespaces();

        try {
            namespaces.asyncGetRequest(admin.getRoot().path(path), Object.class).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(PulsarAdminException.class);
            return ((PulsarAdminException) e.getCause()).getHttpError();
        }
        throw new AssertionError("Request to " + path + " should have failed");
    }

    @Test
    public void testJsonErrorData() throws Exception {
        assertThat(httpErrorFor("/json", aResponse()
                .withStatus(404)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"reason\":\"Namespace does not exist\"}")))
                .isEqualTo("Namespace does not exist");
    }

    @Test
    public void testErrorDataWithoutJsonContentType() throws Exception {
        assertThat(httpErrorFor("/no-content-type", aResponse()
                .withStatus(409)
                .withBody("{\"reason\":\"Topic already exists\"}")))
                .isEqualTo("Topic already exists");
    }

    @Test
    public void testPlainTextBody() throws Exception {
        // AsyncHttpConnector uses the body as the reason phrase, so it ends up in the exception message
        assertThat(httpErrorFor("/text", aResponse()
                .withStatus(400)
                .withHeader("Content-Type", "text/plain")
                .withBody("Unknown property foo")))
                .isEqualTo("HTTP 400 Unknown property foo");
    }

    @Test
    public void testOutboundResponseWithStringEntity() {
        // FunctionsImpl, SinksImpl, SourcesImpl and PackagesImpl build such responses from the raw body
        Response response = Response.status(409).entity("{\"reason\":\"Function already exists\"}").build();
        assertThat(BaseResource.getReasonFromServer(new ClientErrorException(response)))
                .isEqualTo("Function already exists");
    }

    @Test
    public void testOutboundResponseWithoutEntity() {
        assertThat(BaseResource.getReasonFromServer(new ClientErrorException(Response.status(404).build())))
                .isEqualTo("HTTP 404 Not Found");
    }

    @DataProvider
    public Object[][] adminRequests() {
        String persistentTopic = "persistent://tenant/ns/topic";
        String nonPersistentTopic = "non-persistent://tenant/ns/topic";
        return new Object[][] {
                request("getTenantsAsync", admin -> admin.tenants().getTenantsAsync()),
                request("listSinksAsync", admin -> admin.sinks().listSinksAsync("tenant", "ns")),
                request("getStatsAsync (persistent)", admin -> admin.topics().getStatsAsync(persistentTopic)),
                request("getStatsAsync (non-persistent)", admin -> admin.topics().getStatsAsync(nonPersistentTopic)),
                request("getPartitionedStatsAsync (persistent)",
                        admin -> admin.topics().getPartitionedStatsAsync(persistentTopic, false)),
                request("getPartitionedStatsAsync (non-persistent)",
                        admin -> admin.topics().getPartitionedStatsAsync(nonPersistentTopic, false)),
                request("peekMessagesAsync", admin -> admin.topics().peekMessagesAsync(persistentTopic, "sub", 1)),
                request("examineMessageAsync",
                        admin -> admin.topics().examineMessageAsync(persistentTopic, "earliest", 1)),
                request("getMessagesByIdAsync", admin -> admin.topics().getMessagesByIdAsync(persistentTopic, 1, 1)),
                request("healthcheckAsync", admin -> admin.brokers().healthcheckAsync()),
                request("getBuiltInFunctionsAsync", admin -> admin.functions().getBuiltInFunctionsAsync()),
                request("analyzeSubscriptionBacklogAsync",
                        admin -> admin.topics()
                                .analyzeSubscriptionBacklogAsync(persistentTopic, "sub", Optional.empty())),
                request("removePropertyAsync", admin -> admin.namespaces().removePropertyAsync("tenant/ns", "key"))
        };
    }

    @Test(dataProvider = "adminRequests")
    public void testRequestPreservesAuthenticationFailure(String requestName, AdminRequest adminRequest)
            throws Exception {
        AuthenticationException authenticationFailure = new AuthenticationException("Credential unavailable");
        Authentication failingAuthentication = new AuthenticationDisabled() {
            @Override
            public AuthenticationDataProvider getAuthData(String host) throws AuthenticationException {
                throw authenticationFailure;
            }
        };
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl).authentication(failingAuthentication).build()) {
            CompletableFuture<?> requestFuture = adminRequest.invoke(admin);
            assertThatThrownBy(() -> requestFuture.get(3, TimeUnit.SECONDS))
                    .as("%s should preserve the authentication failure", requestName)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(GettingAuthenticationDataException.class)
                    .hasRootCause(authenticationFailure);
        }
    }

    @Test
    public void testCallbackPreservesFailureWithoutCause() {
        FutureCallback<Void> callback = new FutureCallback<>() {};
        IllegalStateException transportFailure = new IllegalStateException("Transport failed");
        callback.failed(transportFailure);

        assertThatThrownBy(() -> callback.future().get(3, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause().isInstanceOf(PulsarAdminException.class)
                .cause().isSameAs(transportFailure);
    }

    @DataProvider
    public Object[][] httpErrors() {
        return new Object[][] {
                {403, NotAuthorizedException.class},
                {409, ConflictException.class}
        };
    }

    @Test(dataProvider = "httpErrors")
    public void testGetMapsHttpFailure(int httpStatusCode, Class<? extends PulsarAdminException> expectedExceptionType)
            throws Exception {
        server.stubFor(get(urlEqualTo("/admin/v2/tenants")).willReturn(aResponse()
                .withStatus(httpStatusCode).withHeader("Content-Type", "application/json")
                .withBody("{\"reason\":\"Server supplied reason\"}")));
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            CompletableFuture<?> requestFuture = admin.tenants().getTenantsAsync();
            assertThatThrownBy(() -> requestFuture.get(3, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause().isInstanceOfSatisfying(expectedExceptionType, adminException -> {
                        assertThat(adminException.getStatusCode()).isEqualTo(httpStatusCode);
                        assertThat(adminException.getHttpError()).isEqualTo("Server supplied reason");
                    });
        }
    }

    private static Object[] request(String requestName, AdminRequest adminRequest) {
        return new Object[] {requestName, adminRequest};
    }

    @FunctionalInterface
    public interface AdminRequest {
        CompletableFuture<?> invoke(PulsarAdmin admin) throws PulsarAdminException;
    }
}
