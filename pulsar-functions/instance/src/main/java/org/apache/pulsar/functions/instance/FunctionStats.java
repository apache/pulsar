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
package org.apache.pulsar.functions.instance;

import com.google.common.collect.EvictingQueue;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.proto.InstanceCommunication;

/**
 * Function stats.
 */
@Slf4j
@Getter
@Setter
public class FunctionStats {

    private static final String[] metricsLabelNames = {"tenant", "namespace", "name", "instance_id"};

    /** Declare Prometheus stats **/

    final Counter statTotalProcessed;

    final Counter statTotalProcessedSuccessfully;

    final Counter statTotalSysExceptions;

    final Counter statTotalUserExceptions;

    final Summary statProcessLatency;

    CollectorRegistry functionCollectorRegistry;

    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions = EvictingQueue.create(10);
    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSystemExceptions = EvictingQueue.create(10);

    @Getter
    @Setter
    private long lastInvocationTime = 0;

    public FunctionStats() {
        // Declare function local collector registry so that it will not clash with other function instances'
        // metrics collection especially in threaded mode
        functionCollectorRegistry = new CollectorRegistry();

        statTotalProcessed = Counter.build()
                .name("__function_total_processed__")
                .help("Total number of messages processed.")
                .labelNames(metricsLabelNames)
                .register(functionCollectorRegistry);

        statTotalProcessedSuccessfully = Counter.build()
                .name("__function_total_successfully_processed__")
                .help("Total number of messages processed successfully.")
                .labelNames(metricsLabelNames)
                .register(functionCollectorRegistry);

        statTotalSysExceptions = Counter.build()
                .name("__function_total_system_exceptions__")
                .help("Total number of system exceptions.")
                .labelNames(metricsLabelNames)
                .register(functionCollectorRegistry);

        statTotalUserExceptions = Counter.build()
                .name("__function_total_user_exceptions__")
                .help("Total number of user exceptions.")
                .labelNames(metricsLabelNames)
                .register(functionCollectorRegistry);

        statProcessLatency = Summary.build()
                .name("__function_process_latency_ms__").help("Process latency in milliseconds.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(metricsLabelNames)
                .register(functionCollectorRegistry);
    }

    public void addUserException(Exception ex) {
        InstanceCommunication.FunctionStatus.ExceptionInformation info =
                    InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                    .setExceptionString(ex.getMessage()).setMsSinceEpoch(System.currentTimeMillis()).build();
        latestUserExceptions.add(info);
    }

    public void addSystemException(Throwable ex) {
        InstanceCommunication.FunctionStatus.ExceptionInformation info =
                InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(ex.getMessage()).setMsSinceEpoch(System.currentTimeMillis()).build();
        latestSystemExceptions.add(info);

    }

    public void reset() {
        statTotalProcessed.clear();
        statTotalProcessedSuccessfully.clear();
        statTotalSysExceptions.clear();
        statTotalUserExceptions.clear();
        statProcessLatency.clear();
        latestUserExceptions.clear();
        latestSystemExceptions.clear();
        lastInvocationTime = 0;
    }
}
