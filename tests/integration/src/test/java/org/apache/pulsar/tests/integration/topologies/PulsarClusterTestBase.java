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
package org.apache.pulsar.tests.integration.topologies;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

@Slf4j
public abstract class PulsarClusterTestBase extends PulsarTestBase {

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

    @DataProvider(name = "ServiceAndAdminUrls")
    public static Object[][] serviceAndAdminUrls() {
        return new Object[][] {
                // plain text
                {
                        pulsarCluster.getPlainTextServiceUrl(),
                        pulsarCluster.getHttpServiceUrl()
                }
        };
    }

    protected static PulsarCluster pulsarCluster;

    public void setupCluster() throws Exception {
        this.setupCluster("");
    }

    public void setupCluster(String namePrefix) throws Exception {
        String clusterName = Stream.of(this.getClass().getSimpleName(), namePrefix, randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder = PulsarClusterSpec.builder()
                .clusterName(clusterName);

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
        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);

        beforeStartCluster();

        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    public void tearDownCluster() {
        if (null != pulsarCluster) {
            pulsarCluster.stop();
        }
    }

}
