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
package org.apache.pulsar.client.api;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class TlsHostVerificationTest extends TlsProducerConsumerBase {

    @Override
    @Test(enabled = false)
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder builder) {
        builder.configCustomizer(config -> {
            // Advertise a hostname that routes but is not on the certificate
            // Note that if you are on a Mac, you'll need to run the following to make loopback work for 127.0.0.2
            // $ sudo ifconfig lo0 alias 127.0.0.2 up
            config.setAdvertisedAddress("127.0.0.2");
        });
    }

    @Test
    public void testTlsHostVerificationAdminClient() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));
        Assert.assertTrue(pulsar.getWebServiceAddressTls().startsWith("https://127.0.0.2:"),
                "Test relies on this address");
        PulsarAdmin adminClientTls = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddressTls())
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH).allowTlsInsecureConnection(false)
                .authentication(AuthenticationTls.class.getName(), authParams).enableTlsHostnameVerification(true)
                .requestTimeout(1, java.util.concurrent.TimeUnit.SECONDS)
                .build();

        try {
            adminClientTls.tenants().getTenants();
            Assert.fail("Admin call should be failed due to hostnameVerification enabled");
        } catch (PulsarAdminException.TimeoutException e) {
            // The test was previously able to fail here, but that is not the right way for the test to pass.
            // If you hit this error and are running on OSX, you may need to run "sudo ifconfig lo0 alias 127.0.0.2 up"
            Assert.fail("Admin call should not timeout, it should fail due to SSL error");
        } catch (PulsarAdminException e) {
            // Ok
        }
    }

    @Test
    public void testTlsHostVerificationDisabledAdminClient() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));
        Assert.assertTrue(pulsar.getWebServiceAddressTls().startsWith("https://127.0.0.2:"),
                "Test relies on this address");
        PulsarAdmin adminClient = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddressTls())
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH).allowTlsInsecureConnection(false)
                .authentication(AuthenticationTls.class.getName(), authParams).enableTlsHostnameVerification(false)
                .build();

        // Should not fail, since verification is disabled
        adminClient.tenants().getTenants();
    }
}
