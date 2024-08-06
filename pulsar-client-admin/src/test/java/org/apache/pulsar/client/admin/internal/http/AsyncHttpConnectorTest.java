package org.apache.pulsar.client.admin.internal.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.internal.PropertiesDelegate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AsyncHttpConnectorTest {
    WireMockServer server;

    @BeforeClass(alwaysRun = true)
    void beforeClass() throws IOException {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig()
                .port(0));
        server.start();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    static class TestClientRequest extends ClientRequest {
        public TestClientRequest(URI uri, ClientConfig clientConfig, PropertiesDelegate propertiesDelegate) {
            super(uri, clientConfig, propertiesDelegate);
        }
    }

    @Test
    public void testShouldStopRetriesWhenTimeoutOccurs() throws IOException, ExecutionException, InterruptedException {
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("once")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("next")
                .willReturn(aResponse()
                        .withFixedDelay(1000)
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")));

        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                        .inScenario("once")
                        .whenScenarioStateIs("next")
                        .willSetStateTo("retried")
                .willReturn(aResponse().withStatus(500)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());

        int readTimeoutMs = 500;
        @Cleanup
        AsyncHttpConnector connector = new AsyncHttpConnector(5000, readTimeoutMs,
                5000, 0, conf, false);

        JerseyClient jerseyClient = JerseyClientBuilder.createClient();
        ClientConfig clientConfig = jerseyClient.getConfiguration();
        PropertiesDelegate propertiesDelegate = new MapPropertiesDelegate();
        URI requestUri = URI.create("http://localhost:" + server.port() + "/admin/v2/clusters");
        ClientRequest request = new TestClientRequest(requestUri, clientConfig, propertiesDelegate);
        request.setMethod("GET");
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();
        connector.apply(request, new AsyncConnectorCallback() {
            @Override
            public void response(ClientResponse response) {
                future.complete(response);
            }

            @Override
            public void failure(Throwable failure) {
                future.completeExceptionally(failure);
            }
        });
        Thread.sleep(2 * readTimeoutMs);
        String scenarioState =
                server.getAllScenarios().getScenarios().stream().filter(scenario -> "once".equals(scenario.getName()))
                        .findFirst().get().getState();
        assertEquals(scenarioState, "next");
        assertTrue(future.isCompletedExceptionally());
    }
}