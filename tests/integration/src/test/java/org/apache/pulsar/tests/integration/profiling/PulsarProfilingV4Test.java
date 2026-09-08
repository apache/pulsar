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
package org.apache.pulsar.tests.integration.profiling;

import java.util.List;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.annotations.Test;

/**
 * Profiles the broker while pulsar-perf drives a classic v4 topic with the v4 client.
 *
 * Run it with {@code ./gradlew :tests:integration:profilingIntegrationTest --tests
 * "*PulsarProfilingV4Test"}. It is the pre-v5 baseline for {@link PulsarProfilingTest}: same
 * cluster, same load parameters, only the client generation and the topic domain differ. See
 * {@link AbstractPulsarProfilingTest} for the rest.
 */
public class PulsarProfilingV4Test extends AbstractPulsarProfilingTest {

    @Override
    protected TopicDomain getTopicDomain() {
        return TopicDomain.persistent;
    }

    @Override
    protected String getPerfCommandSuffix() {
        return "-v4";
    }

    @Override
    protected List<TopicStatsEndpoint> getTopicStatsEndpoints(String topicName) {
        String basePath = adminV2Path("persistent", topicName);
        return List.of(
                new TopicStatsEndpoint("stats", basePath + "/stats"),
                new TopicStatsEndpoint("internal_stats", basePath + "/internalStats"));
    }

    @Test(timeOut = 600_000)
    public void runPulsarPerf() throws Exception {
        runPulsarPerfBenchmark();
    }
}
