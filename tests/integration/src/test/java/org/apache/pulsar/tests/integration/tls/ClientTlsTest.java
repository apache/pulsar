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
package org.apache.pulsar.tests.integration.tls;

import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClientTlsTest extends PulsarTestSuite {
    private final static String tlsTrustCertsFilePath = loadCertificateAuthorityFile("certs/ca.cert.pem");
    private final static String tlsKeyFilePath = loadCertificateAuthorityFile("client-keys/admin.key-pk8.pem");
    private final static String tlsCertificateFilePath = loadCertificateAuthorityFile("client-keys/admin.cert.pem");

    private static String loadCertificateAuthorityFile(String name) {
        return Resources.getResource("certificate-authority/" + name).getPath();
    }

    @DataProvider(name = "adminUrls")
    public Object[][] adminUrls() {
        return new Object[][]{
                {stringSupplier(() -> getPulsarCluster().getAnyBrokersHttpsServiceUrl())},
                {stringSupplier(() -> getPulsarCluster().getProxy().getHttpsServiceUrl())}
        };
    }

    @DataProvider(name = "serviceUrls")
    public Object[][] serviceUrls() {
        return new Object[][]{
                {stringSupplier(() -> getPulsarCluster().getProxy().getServiceUrlTls())},
        };
    }

    @Test(dataProvider = "adminUrls")
    public void testAdmin(Supplier<String> urlSupplier) throws PulsarAdminException, PulsarClientException {
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(urlSupplier.get())
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .tlsKeyFilePath(tlsKeyFilePath)
                .tlsCertificateFilePath(tlsCertificateFilePath)
                .build();
        admin.tenants().getTenants();
    }

    @Test(dataProvider = "serviceUrls")
    public void testClient(Supplier<String> urlSupplier) throws PulsarClientException {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(urlSupplier.get())
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .tlsKeyFilePath(tlsKeyFilePath)
                .tlsCertificateFilePath(tlsCertificateFilePath)
                .build();
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(UUID.randomUUID().toString()).create();
        producer.send("Hello".getBytes(StandardCharsets.UTF_8));
    }
}
