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
package org.apache.pulsar.tests.integration.upgrade;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.dockerjava.api.model.Volume;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarStandaloneUpgradeTest {
    private static final String TOPIC = "persistent://public/default/standalone-upgrade";

    @DataProvider
    public Object[][] previousReleases() {
        return new Object[][] {
                {"apachepulsar/pulsar:4.0.13", true},
                {"apachepulsar/pulsar:4.2.4", true},
                {"apachepulsar/pulsar:5.0.0-M2", false}
        };
    }

    @Test(dataProvider = "previousReleases", timeOut = 900_000)
    public void testUpgradeWithExistingCookies(String oldImage, boolean legacyAddress) throws Exception {
        String clusterName = "standalone-upgrade-" + UUID.randomUUID();
        try (Network network = Network.newNetwork();
             GenericContainer<?> storage = new GenericContainer<>(PulsarContainer.ALPINE_IMAGE_NAME)
                     .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(new Volume("/pulsar/data")))
                     .withCommand("sh", "-c", "chown 10000:0 /pulsar/data && chmod 775 /pulsar/data"
                             + " && echo ready && tail -f /dev/null")
                     .waitingFor(Wait.forLogMessage(".*ready\n", 1))) {
            storage.start();
            String[] cookies = new String[2];
            try (StandaloneContainer old = standalone(clusterName, oldImage, network, storage, null)) {
                old.start();
                old.execCmd("bin/pulsar-admin", "topics", "create", TOPIC);
                // Keep a durable subscription unread so every phase can verify all persisted messages.
                old.execCmd("bin/pulsar-admin", "topics", "create-subscription", "-s", "upgrade", TOPIC);
                produce(old, "before-upgrade");
                for (int i = 0; i < cookies.length; i++) {
                    cookies[i] = cookie(old, i);
                    if (legacyAddress) {
                        assertThat(cookies[i]).containsPattern("bookieHost: \"[^\"]+:[0-9]+\"");
                    } else {
                        assertThat(cookies[i]).containsPattern("bookieHost: \"bk-" + i + "-[0-9a-f]+\"");
                    }
                }
            }
            try (StandaloneContainer upgraded = standalone(clusterName, PulsarContainer.UPGRADE_TEST_IMAGE_NAME,
                    network, storage, null)) {
                upgraded.start();
                if (legacyAddress) {
                    assertBookiePorts(upgraded, cookies, true, 0);
                }
                assertCookiesUnchanged(upgraded, cookies);
                consume(upgraded, "before-upgrade");
                produce(upgraded, "after-upgrade");
            }
            try (StandaloneContainer restarted = standalone(clusterName, PulsarContainer.UPGRADE_TEST_IMAGE_NAME,
                    network, storage, 3191)) {
                restarted.start();
                assertBookiePorts(restarted, cookies, legacyAddress, 3191);
                assertCookiesUnchanged(restarted, cookies);
                consume(restarted, "before-upgrade", "after-upgrade");
                produce(restarted, "after-restart");
                consume(restarted, "before-upgrade", "after-upgrade", "after-restart");
            }
        }
    }

    @DataProvider
    public Object[][] standaloneModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "standaloneModes", timeOut = 300_000)
    public void testExplicitBookiePorts(boolean useZookeeper) throws Exception {
        try (Network network = Network.newNetwork();
             StandaloneContainer container = standalone("standalone-ports-" + UUID.randomUUID(),
                     PulsarContainer.UPGRADE_TEST_IMAGE_NAME, network, null, 3181)
                     .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", Boolean.toString(useZookeeper))) {
            container.start();
            assertListening(container, 3181);
            assertListening(container, 3182);
            if (!useZookeeper) {
                for (int i = 0; i < 2; i++) {
                    assertThat(cookie(container, i)).contains("bookieHost: \"bk-" + i + "\"");
                }
            }
            container.execCmd("bin/pulsar-admin", "topics", "create", TOPIC);
            container.execCmd("bin/pulsar-admin", "topics", "create-subscription", "-s", "upgrade", TOPIC);
            produce(container, "fixed-ports");
            consume(container, "fixed-ports");
        }
    }

    private static void assertBookiePorts(StandaloneContainer container, String[] cookies,
                                         boolean legacyAddress, int basePort) throws Exception {
        for (int i = 0; i < cookies.length; i++) {
            int port = basePort + i;
            if (legacyAddress) {
                Matcher matcher = Pattern.compile("bookieHost: \"[^\"]+:(\\d+)\"").matcher(cookies[i]);
                assertThat(matcher.find()).isTrue();
                port = Integer.parseInt(matcher.group(1));
            }
            assertListening(container, port);
        }
    }

    private static void assertListening(StandaloneContainer container, int port) throws Exception {
        container.execCmd("bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/" + port);
    }

    private static StandaloneContainer standalone(String clusterName, String image, Network network,
                                                   GenericContainer<?> storage, Integer basePort) {
        StandaloneContainer container = new StandaloneContainer(clusterName, image) {
            @Override
            protected void configure() {
                super.configure();
                if (basePort == null) {
                    setCommand("standalone", "--num-bookies", "2", "--no-functions-worker");
                } else {
                    setCommand("standalone", "--num-bookies", "2", "--no-functions-worker",
                            "--bookkeeper-port", basePort.toString());
                }
            }
        }.withNetwork(network)
                .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", "false");
        if (storage != null) {
            container.withVolumesFrom(storage, BindMode.READ_WRITE);
        }
        return container;
    }

    private static String cookie(StandaloneContainer container, int index) throws Exception {
        String directory = "/pulsar/data/standalone/bookkeeper" + (index == 0 ? "" : "/" + index);
        return container.execCmd("cat", directory + "/current/VERSION").getStdout();
    }

    private static void assertCookiesUnchanged(StandaloneContainer container, String[] cookies) throws Exception {
        for (int i = 0; i < cookies.length; i++) {
            assertThat(cookie(container, i)).isEqualTo(cookies[i]);
        }
    }

    private static void produce(StandaloneContainer container, String message) throws Exception {
        container.execCmd("bin/pulsar-client", "produce", TOPIC, "-m", message);
    }

    private static void consume(StandaloneContainer container, String... messages) throws Exception {
        ContainerExecResult result = container.execCmdAsync("bin/pulsar-client", "consume", TOPIC,
                "-s", "read-" + UUID.randomUUID(), "-p", "Earliest", "-n", Integer.toString(messages.length))
                .get(60, TimeUnit.SECONDS);
        for (String message : messages) {
            assertThat(result.getStdout()).contains("content:" + message);
        }
    }
}
