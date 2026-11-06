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
package org.apache.pulsar.broker.web;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.util.concurrent.DefaultThreadFactory;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.ext.WriterInterceptor;
import jakarta.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests that the REST resources that {@link WebService} adds, like the broker's own resources, write the responses
 * of async endpoints on the web service executor.
 */
@Test(groups = "broker")
public class WebServiceAsyncResponseTest extends MockedPulsarServiceBaseTest {

    private static final String OTHER_THREAD_PREFIX = "test-other";
    private static final String WRITER_THREAD_HEADER = "X-Writer-Thread";

    private static ExecutorService otherExecutor;
    private WebService webService;
    private HttpClient httpClient;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        otherExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory(OTHER_THREAD_PREFIX));
        // a second web service of the test broker, since resources can't be added once a web service has started
        Optional<Integer> brokerWebServicePort = conf.getWebServicePort();
        conf.setWebServicePort(Optional.of(0));
        try {
            webService = new WebService(pulsar);
            webService.addRestResource("/", false, Map.of(), true, TestResource.class,
                    WriterThreadInterceptor.class);
            webService.start();
        } finally {
            // the web service sets the port that it listens on in the configuration
            conf.setWebServicePort(brokerWebServicePort);
        }
        httpClient = HttpClient.newHttpClient();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
        if (webService != null) {
            webService.close();
        }
        if (otherExecutor != null) {
            otherExecutor.shutdownNow();
        }
        super.internalCleanup();
    }

    @Path("/test")
    public static class TestResource {
        @GET
        @Path("/resumeFromOtherThread")
        public void resumeFromOtherThread(@Suspended AsyncResponse asyncResponse) {
            otherExecutor.execute(() -> asyncResponse.resume("resumed"));
        }
    }

    public static class WriterThreadInterceptor implements WriterInterceptor {
        @Override
        public void aroundWriteTo(WriterInterceptorContext context) throws IOException {
            context.getHeaders().add(WRITER_THREAD_HEADER, Thread.currentThread().getName());
            context.proceed();
        }
    }

    @Test
    public void testResumeFromOtherThreadWritesOnWebServiceExecutor() throws Exception {
        URI uri = URI.create("http://localhost:" + webService.getListenPortHTTP().orElseThrow()
                + "/test/resumeFromOtherThread");
        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder(uri).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("resumed");
        assertThat(response.headers().firstValue(WRITER_THREAD_HEADER)).hasValueSatisfying(
                threadName -> assertThat(threadName).startsWith("pulsar-web"));
    }
}
