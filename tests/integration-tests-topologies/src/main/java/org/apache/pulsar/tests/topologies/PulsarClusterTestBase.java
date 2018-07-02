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
package org.apache.pulsar.tests.topologies;

import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

@Slf4j
public class PulsarClusterTestBase {

    @DataProvider(name = "ServiceUrlAndTopics")
    public static Object[][] serviceUrlAndTopics() {
        return new Object[][] {
            // plain text, persistent topic
            {
                pulsarCluster.getPlainTextServiceUrl(),
                true,
            },
            // plain text, non-persistent topic
            {
                pulsarCluster.getPlainTextServiceUrl(),
                false
            }
        };
    }

    @DataProvider(name = "ServiceUrls")
    public static Object[][] serviceUrls() {
        return new Object[][] {
            // plain text
            {
                pulsarCluster.getPlainTextServiceUrl()
            }
        };
    }

    protected static PulsarCluster pulsarCluster;

    @BeforeClass
    public static void setupCluster() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .clusterName(sb.toString())
            .build();

        setupCluster(spec);
    }

    protected static void setupCluster(PulsarClusterSpec spec) throws Exception {
        log.info("Setting up cluster {} with {} bookies, {} brokers",
            spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterClass
    public static void teardownCluster() {
        if (null != pulsarCluster) {
            pulsarCluster.stop();
        }
    }

    protected static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();;;;
        for (int i = 0; i < 8; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    protected static String generateNamespaceName() {
        return "ns-" + randomName(8);
    }

    protected static String generateTopicName(String topicPrefix, boolean isPersistent) {
        return generateTopicName("default", topicPrefix, isPersistent);
    }

    protected static String generateTopicName(String namespace, String topicPrefix, boolean isPersistent) {
        String topicName = new StringBuilder(topicPrefix)
            .append("-")
            .append(randomName(8))
            .append("-")
            .append(System.currentTimeMillis())
            .toString();
        if (isPersistent) {
            return "persistent://public/" + namespace + "/" + topicName;
        } else {
            return "non-persistent://public/" + namespace + "/" + topicName;
        }
    }



}
