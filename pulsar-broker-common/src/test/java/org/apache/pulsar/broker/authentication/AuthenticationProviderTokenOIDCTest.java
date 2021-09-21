/**
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

package org.apache.pulsar.broker.authentication;

import com.github.tomakehurst.wiremock.WireMockServer;

import org.apache.pulsar.broker.ServiceConfiguration;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import javax.net.ssl.*;

public class AuthenticationProviderTokenOIDCTest {

    WireMockServer wm;

    @BeforeTest
    public void before(){

        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HostnameVerifier allHostsValid = (hostname, session) -> true;

            // set the  allTrusting verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (Exception e) {
        }


        wm = new WireMockServer(options().httpsPort(8443));
        //String issuerUrl = wireMockRule.baseUrl();

        wm.stubFor(get(urlPathMatching("/.well-known/openid-configuration"))
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"jwks_uri\": \"https://www.test.com\"}")));

        wm.start();
    }

    @Test
    public void checkInvalidUrl() throws IOException {
        AuthenticationProviderTokenOIDC provider = new AuthenticationProviderTokenOIDC();
        ServiceConfiguration conf = new ServiceConfiguration();
        Properties properties = new Properties();
        //fail(wm.baseUrl());
        properties.setProperty(AuthenticationProviderTokenOIDC.CONF_ISSUER_URL, wm.baseUrl());

        conf.setProperties(properties);

        provider.initialize(conf);

        provider.close();


    }

}
