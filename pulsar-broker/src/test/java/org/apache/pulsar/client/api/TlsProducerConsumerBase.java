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
package org.apache.pulsar.client.api;

import static org.mockito.Mockito.spy;

import java.net.URI;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TlsProducerConsumerBase extends ProducerConsumerBase {
    protected final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";
    protected final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    protected final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String clusterName = "use";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {

        // TLS configuration for Broker
        internalSetUpForBroker();

        // Start Broker
        super.init();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void internalSetUpForBroker() throws Exception {
        conf.setTlsEnabled(true);
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setClusterName(clusterName);
    }

    protected void internalSetUpForClient() throws Exception {
        String lookupUrl = new URI("pulsar+ssl://localhost:" + BROKER_PORT_TLS).toString();
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl).tlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH)
                .enableTls(true).build();
    }

    protected void internalSetUpForNamespace() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        clientConf.setUseTls(true);
        admin = spy(new PulsarAdmin(brokerUrlTls, clientConf));
        admin.clusters().updateCluster(clusterName, new ClusterData(brokerUrl.toString(), brokerUrlTls.toString(),
                "pulsar://localhost:" + BROKER_PORT, "pulsar+ssl://localhost:" + BROKER_PORT_TLS));
        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }
}