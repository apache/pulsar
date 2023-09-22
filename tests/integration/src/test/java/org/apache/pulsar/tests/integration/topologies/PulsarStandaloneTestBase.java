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
package org.apache.pulsar.tests.integration.topologies;

import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTPS_PORT;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_PORT_TLS;
import static org.testng.Assert.assertEquals;
import com.google.common.io.Resources;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.testcontainers.containers.Network;
import org.testng.annotations.DataProvider;

/**
 * A test base to run tests on standalone cluster.
 *
 * <p>Ideally we should run all integration tests on both cluster mode and standalone
 * mode. However the apache ci can't really afford to do so. so we run all the integration
 * tests on cluster mode. We only run basic validation and test new features (e.g. state)
 * on standalone.
 */
@Slf4j
public abstract class PulsarStandaloneTestBase extends PulsarTestBase {

    protected final static String clientTlsTrustCertsFilePath = loadCertificateAuthorityFile("certs/ca.cert.pem");
    protected final static String clientTlsKeyFilePath = loadCertificateAuthorityFile("client-keys/admin.key-pk8.pem");
    protected final static String clientTlsCertificateFilePath = loadCertificateAuthorityFile("client-keys/admin.cert.pem");


    private static String loadCertificateAuthorityFile(String name) {
        return Resources.getResource("certificate-authority/" + name).getPath();
    }

    protected final Map<String, String> standaloneEnvs = new HashMap<>();

    @DataProvider(name = "StandaloneServiceUrlAndTopics")
    public Object[][] serviceUrlAndTopics() {
        return new Object[][] {
                // plain text, persistent topic
                {
                        stringSupplier(() -> getContainer().getPlainTextServiceUrl()),
                        true,
                },
                // plain text, non-persistent topic
                {
                        stringSupplier(() -> getContainer().getPlainTextServiceUrl()),
                        false
                }
        };
    }

    @DataProvider(name = "StandaloneServiceUrlAndHttpUrl")
    public Object[][] serviceUrlAndHttpUrl() {
        return new Object[][] {
                {
                        stringSupplier(() -> getContainer().getPlainTextServiceUrl()),
                        stringSupplier(() -> getContainer().getHttpServiceUrl()),
                }
        };
    }

    protected Network network;

    protected StandaloneContainer container;

    public StandaloneContainer getContainer() {
        return container;
    }

    private static Supplier<String> stringSupplier(Supplier<String> supplier) {
        return supplier;
    }

    protected void startCluster(final String pulsarImageName) throws Exception {
        network = Network.newNetwork();
        String clusterName = PulsarClusterTestBase.randomName(8);
        container = new StandaloneContainer(clusterName, pulsarImageName)
            .withNetwork(network)
            .withNetworkAliases(StandaloneContainer.NAME + "-" + clusterName)
            .withEnv("PF_stateStorageServiceUrl", "bk://localhost:4181")
            .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", "true");
        container.withEnv(standaloneEnvs);
        container.start();
        log.info("Pulsar cluster {} is up running:", clusterName);
        log.info("\tBinary Service Url : {}", container.getPlainTextServiceUrl());
        log.info("\tHttp Service Url : {}", container.getHttpServiceUrl());

        // add cluster to public tenant
        ContainerExecResult result = container.execCmd(
                "/pulsar/bin/pulsar-admin", "namespaces", "policies", "public/default");
        assertEquals(0, result.getExitCode());
        log.info("public/default namespace policies are {}", result.getStdout());
    }

    protected void stopCluster() throws Exception {
        if (container != null) {
            container.stop();
            container = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }



    protected void dumpFunctionLogs(String name) {
        try {
            String logFile = "/pulsar/logs/functions/public/default/" + name + "/" + name + "-0.log";
            String logs = container.<String>copyFileFromContainer(logFile, (inputStream) -> {
                return IOUtils.toString(inputStream, "utf-8");
            });
            log.info("Function {} logs {}", name, logs);
        } catch (Throwable err) {
            log.info("Cannot download {} logs", name, err);
        }
    }

    protected void setupTLS () {
        standaloneEnvs.put("PULSAR_PREFIX_webServicePortTls", String.valueOf(BROKER_HTTPS_PORT));
        standaloneEnvs.put("PULSAR_PREFIX_brokerServicePortTls", String.valueOf(BROKER_PORT_TLS));
        standaloneEnvs.put("PULSAR_PREFIX_brokerClientTlsEnabled", "true");
        standaloneEnvs.put("PULSAR_PREFIX_tlsRequireTrustedClientCertOnConnect", "true");
        standaloneEnvs.put("PULSAR_PREFIX_tlsAllowInsecureConnection", "false");
        standaloneEnvs.put("PULSAR_PREFIX_tlsCertificateFilePath",
                "/pulsar/certificate-authority/server-keys/broker.cert.pem");
        standaloneEnvs.put("PULSAR_PREFIX_tlsKeyFilePath",
                "/pulsar/certificate-authority/server-keys/broker.key-pk8.pem");
        standaloneEnvs.put("PULSAR_PREFIX_tlsTrustCertsFilePath",
                "/pulsar/certificate-authority/certs/ca.cert.pem");
        standaloneEnvs.put("PULSAR_PREFIX_brokerClientCertificateFilePath",
                "/pulsar/certificate-authority/client-keys/admin.cert.pem");
        standaloneEnvs.put("PULSAR_PREFIX_brokerClientKeyFilePath",
                "/pulsar/certificate-authority/client-keys/admin.key-pk8.pem");
        standaloneEnvs.put("PULSAR_PREFIX_brokerClientTrustCertsFilePath",
                "/pulsar/certificate-authority/certs/ca.cert.pem");
        standaloneEnvs.put("PULSAR_PREFIX_brokerClientAuthenticationPlugin",
                "org.apache.pulsar.client.impl.auth.AuthenticationTls");
        standaloneEnvs.put("PULSAR_PREFIX_brokerClientAuthenticationParameters",
                "{\"tlsCertFile\":\"/pulsar/certificate-authority/client-keys/admin.cert.pem\","
                        + "\"tlsKeyFile\":\"/pulsar/certificate-authority/client-keys/admin.key-pk8.pem\"}");
    }
}
