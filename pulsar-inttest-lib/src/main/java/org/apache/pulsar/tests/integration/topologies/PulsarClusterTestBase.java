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

import static java.util.stream.Collectors.joining;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.annotations.DataProvider;

@Slf4j
public abstract class PulsarClusterTestBase extends PulsarTestBase {

    public static final String CLIENT_CONFIG_FILE_PATH_PROPERTY_NAME = "client.config.file.path";

    protected final Map<String, String> brokerEnvs = new HashMap<>();
    protected final Map<String, String> bookkeeperEnvs = new HashMap<>();
    protected final Map<String, String> proxyEnvs = new HashMap<>();
    protected final List<Integer> brokerAdditionalPorts = new LinkedList<>();
    protected final List<Integer> bookieAdditionalPorts = new LinkedList<>();


    private Map<String, Object> readClientConfigs(String clientConfFilePath) throws IOException {
        Properties prop = new Properties(System.getProperties());
        try (FileInputStream input = new FileInputStream(clientConfFilePath)) {
            prop.load(input);
        }
        Map<String, Object> map = new HashMap<>();
        for (String key : prop.stringPropertyNames()) {
            map.put(key, prop.get(key));
        }

        return map;
    }

    protected PulsarClient getPulsarClient() throws IOException {
        var clientConfFilePath = System.getProperty(CLIENT_CONFIG_FILE_PATH_PROPERTY_NAME);

        if (clientConfFilePath == null) {
            return PulsarClient.builder().serviceUrl(getPulsarCluster().getPlainTextServiceUrl()).build();
        }

        return PulsarClient.builder().loadConf(readClientConfigs(clientConfFilePath)).build();
    }

    protected PulsarAdmin getPulsarAdmin() throws IOException {
        var clientConfFilePath = System.getProperty(CLIENT_CONFIG_FILE_PATH_PROPERTY_NAME);

        if (clientConfFilePath == null) {
            return PulsarAdmin.builder().serviceHttpUrl(getPulsarCluster().getHttpServiceUrl()).build();
        }
        return PulsarAdmin.builder().loadConf(readClientConfigs(clientConfFilePath)).build();
    }

    @Override
    protected final void setup() throws Exception {
        setupCluster();
    }

    @Override
    protected final void cleanup() throws Exception {
        tearDownCluster();
    }

    @DataProvider(name = "ServiceUrlAndTopics")
    public Object[][] serviceUrlAndTopics() {
        return new Object[][]{
                // plain text, persistent topic
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        true,
                },
                // plain text, non-persistent topic
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        false
                }
        };
    }

    @DataProvider(name = "ServiceUrls")
    public Object[][] serviceUrls() {
        return new Object[][]{
                // plain text
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl())
                }
        };
    }

    @DataProvider(name = "ServiceAndAdminUrls")
    public Object[][] serviceAndAdminUrls() {
        return new Object[][]{
                // plain text
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        stringSupplier(() -> getPulsarCluster().getHttpServiceUrl())
                }
        };
    }

    @DataProvider
    public Object[][] serviceUrlAndTopicDomain() {
        return new Object[][]{
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        TopicDomain.persistent
                },
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        TopicDomain.non_persistent
                },
        };
    }

    @DataProvider(name = "topicDomain")
    public Object[][] topicDomain() {
        return new Object[][]{
                {
                        TopicDomain.persistent
                },
                {
                        TopicDomain.non_persistent
                },
        };
    }

    protected PulsarAdmin pulsarAdmin;

    protected PulsarCluster pulsarCluster;

    public PulsarCluster getPulsarCluster() {
        return pulsarCluster;
    }

    protected static Supplier<String> stringSupplier(Supplier<String> supplier) {
        return supplier;
    }

    public void setupCluster() throws Exception {
        this.setupCluster("");
    }

    public void setupCluster(String namePrefix) throws Exception {
        String clusterName = Stream.of(this.getClass().getSimpleName(), namePrefix, randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerEnvs)
                .proxyEnvs(proxyEnvs)
                .brokerAdditionalPorts(brokerAdditionalPorts);

        setupCluster(beforeSetupCluster(clusterName, specBuilder).build());
    }

    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        return specBuilder;
    }

    protected void beforeStartCluster() throws Exception {
        // no-op
    }

    protected void setupCluster(PulsarClusterSpec spec) throws Exception {
        setupCluster(spec, true);
    }

    protected void setupCluster(PulsarClusterSpec spec, boolean doInit) throws Exception {
        incrementSetupNumber();
        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);

        beforeStartCluster();

        pulsarCluster.start(doInit);

        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    public void tearDownCluster() throws Exception {
        markCurrentSetupNumberCleaned();
        if (null != pulsarCluster) {
            pulsarCluster.stop();
        }
    }
}
