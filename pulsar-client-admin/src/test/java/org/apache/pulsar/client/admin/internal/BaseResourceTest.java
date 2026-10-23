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
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
}
