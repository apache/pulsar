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
 * Profiles the broker while pulsar-perf drives a v5 scalable topic with the v5 client.
 *
 * This is the variant that {@code ./gradlew :tests:integration:profilingIntegrationTest} runs by
 * default. See {@link AbstractPulsarProfilingTest} for how to run it and where the recordings land,
 * and {@link PulsarProfilingV4Test} for the v4 counterpart.
 */
public class PulsarProfilingTest extends AbstractPulsarProfilingTest {

    @Override
    protected TopicDomain getTopicDomain() {
        return TopicDomain.topic;
    }

    @Override
    protected String getPerfCommandSuffix() {
        return "";
    }

    @Override
    protected List<TopicStatsEndpoint> getTopicStatsEndpoints(String topicName) {
        // Scalable topics have their own admin resource: aggregated stats (including per-segment and
        // per-subscription counts) are under /admin/v2/scalable, not /admin/v2/topic, and there is no
        // internalStats equivalent, so the managed-ledger internals of the backing segments are not
        // collected. Broker metrics are collected either way.
        return List.of(new TopicStatsEndpoint("stats", adminV2Path("scalable", topicName) + "/stats"));
    }

    @Test(timeOut = 600_000)
    public void runPulsarPerf() throws Exception {
        runPulsarPerfBenchmark();
    }
}
