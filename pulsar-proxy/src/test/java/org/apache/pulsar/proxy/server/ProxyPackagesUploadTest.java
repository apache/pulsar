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
package org.apache.pulsar.proxy.server;

import static com.google.common.net.HttpHeaders.EXPECT;
import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.eclipse.jetty.servlet.ServletHolder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class ProxyPackagesUploadTest extends MockedPulsarServiceBaseTest {

    private static final int FILE_SIZE = 8 * 1024 * 1024; // 8 MB
    private static final ObjectMapper MAPPER = ObjectMapperFactory.create();
    private WebServer webServer;
    private PulsarAdmin proxyAdmin;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setEnablePackagesManagement(true);
        conf.setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        super.internalSetup();

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerWebServiceURL(brokerUrl.toString());

        webServer = new WebServer(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig, true)));
        webServer.addServlet("/", new ServletHolder(new AdminProxyHandler(proxyConfig, null, null)));
        webServer.start();

        proxyAdmin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:" + webServer.getListenPortHTTP().get())
                .build();

        admin.tenants().createTenant("public", createDefaultTenantInfo());
        admin.namespaces().createNamespace("public/default");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (proxyAdmin != null) {
            proxyAdmin.close();
        }
        if (webServer != null) {
            webServer.stop();
        }
        super.internalCleanup();
    }

    @Test
    public void testUploadPackageThroughProxy() throws Exception {
        Path packageFile = Files.createTempFile("pkg-sdk", ".nar");
        packageFile.toFile().deleteOnExit();
        Files.write(packageFile, new byte[FILE_SIZE]);

        String pkgName = "function://public/default/pkg-sdk@v1";
        PackageMetadata meta = PackageMetadata.builder().description("sdk-test").build();

        proxyAdmin.packages().upload(meta, pkgName, packageFile.toString());

        verifyDownload(pkgName, FILE_SIZE);
    }

    @Test
    public void testUploadWithExpect100Continue() throws Exception {
        Path packageFile = Files.createTempFile("pkg-ahc", ".nar");
        packageFile.toFile().deleteOnExit();
        Files.write(packageFile, new byte[FILE_SIZE]);

        String pkgName = "function://public/default/expect-test@v1";
        String uploadUrl = String.format("http://localhost:%d/admin/v3/packages/function/public/default/expect-test/v1",
                webServer.getListenPortHTTP().orElseThrow());

        @Cleanup
        AsyncHttpClient client = new DefaultAsyncHttpClient(new DefaultAsyncHttpClientConfig.Builder().build());

        Response response = client.executeRequest(new RequestBuilder("POST")
                .setUrl(uploadUrl)
                .addHeader(EXPECT, "100-continue")
                .addBodyPart(new FilePart("file", packageFile.toFile()))
                .addBodyPart(new StringPart("metadata", MAPPER.writeValueAsString(
                        PackageMetadata.builder().description("ahc-test").build()), "application/json"))
                .build()).get();

        assertThat(response.getStatusCode()).isEqualTo(204);

        verifyDownload(pkgName, FILE_SIZE);
    }

    private void verifyDownload(String packageName, int expectedSize) throws Exception {
        Path fromBroker = Files.createTempFile("from-broker", ".nar");
        fromBroker.toFile().deleteOnExit();
        admin.packages().download(packageName, fromBroker.toString());
        assertThat(Files.size(fromBroker)).isEqualTo(expectedSize);
        Files.deleteIfExists(fromBroker);

        Path fromProxy = Files.createTempFile("from-proxy", ".nar");
        fromProxy.toFile().deleteOnExit();
        proxyAdmin.packages().download(packageName, fromProxy.toString());
        assertThat(Files.size(fromProxy)).isEqualTo(expectedSize);
    }
}
