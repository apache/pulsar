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
package org.apache.pulsar.functions.worker;

import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * A class to generate stats for pulsar functions running on this broker
 */
public class FunctionsStatsGenerator {

    private static final Logger log = LoggerFactory.getLogger(FunctionsStatsGenerator.class);

    public static void generate(PulsarWorkerService workerService, SimpleTextOutputStream out) {
        // only when worker service is initialized, we generate the stats. otherwise we will get bunch of NPE.
        if (workerService != null && workerService.isInitialized()) {

            /* worker internal stats */

            try {
                out.write(workerService.getWorkerStatsManager().getStatsAsString());
            } catch (IOException e) {
                log.warn("Encountered error when generating metrics for worker {}",
                  workerService.getWorkerConfig().getWorkerId(), e);
            }

            /* function stats */

            // kubernetes runtime factory doesn't support stats collection through worker service
            if (workerService.getFunctionRuntimeManager().getRuntimeFactory() instanceof KubernetesRuntimeFactory) {
                return;
            }

            Map<String, FunctionRuntimeInfo> functionRuntimes
                    = workerService.getFunctionRuntimeManager().getFunctionRuntimeInfos();

            for (Map.Entry<String, FunctionRuntimeInfo> entry : functionRuntimes.entrySet()) {
                String fullyQualifiedInstanceName = entry.getKey();
                FunctionRuntimeInfo functionRuntimeInfo = entry.getValue();
                RuntimeSpawner functionRuntimeSpawner = functionRuntimeInfo.getRuntimeSpawner();

                if (functionRuntimeSpawner != null) {
                    Runtime functionRuntime = functionRuntimeSpawner.getRuntime();
                    if (functionRuntime != null) {
                        try {

                            String prometheusMetrics = functionRuntime.getPrometheusMetrics();
                            if (prometheusMetrics != null) {
                                out.write(prometheusMetrics);
                            }

                        } catch (IOException e) {
                            log.warn("Failed to collect metrics for function instance {}",
                                    fullyQualifiedInstanceName, e);
                        }
                    }
                }
            }
        }
    }
}
