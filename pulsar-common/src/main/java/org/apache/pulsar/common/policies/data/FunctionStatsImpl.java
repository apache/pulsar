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
package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import lombok.Data;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Statistics for Pulsar Function.
 */
@Data
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({"receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal",
        "avgProcessLatency", "1min", "lastInvocation", "instances"})
public class FunctionStatsImpl implements FunctionStats {

    /**
     * Overall total number of records function received from source.
     **/
    private long receivedTotal;

    /**
     * Overall total number of records successfully processed by user function.
     **/
    private long processedSuccessfullyTotal;

    /**
     * Overall total number of system exceptions thrown.
     **/
    private long systemExceptionsTotal;

    /**
     * Overall total number of user exceptions thrown.
     **/
    private long userExceptionsTotal;

    /**
     * Average process latency for function.
     **/
    private Double avgProcessLatency;

    @JsonProperty("1min")
    private FunctionInstanceStatsDataBaseImpl oneMin =
            new FunctionInstanceStatsDataBaseImpl();

    /**
     * Timestamp of when the function was last invoked by any instance.
     **/
    private Long lastInvocation;

    public List<FunctionInstanceStatsImpl> instances = new LinkedList<>();

    public void addInstance(FunctionInstanceStatsImpl functionInstanceStats) {
        instances.add(functionInstanceStats);
    }

    @Override
    public FunctionStatsImpl calculateOverall() {

        int nonNullInstances = 0;
        int nonNullInstancesOneMin = 0;
        for (FunctionInstanceStats functionInstanceStats : instances) {
            FunctionInstanceStatsDataImpl functionInstanceStatsData =
                    (FunctionInstanceStatsDataImpl) functionInstanceStats.getMetrics();
            receivedTotal += functionInstanceStatsData.getReceivedTotal();
            processedSuccessfullyTotal += functionInstanceStatsData.getProcessedSuccessfullyTotal();
            systemExceptionsTotal += functionInstanceStatsData.getSystemExceptionsTotal();
            userExceptionsTotal += functionInstanceStatsData.getUserExceptionsTotal();
            if (functionInstanceStatsData.getAvgProcessLatency() != null) {
                if (avgProcessLatency == null) {
                    avgProcessLatency = 0.0;
                }
                avgProcessLatency += functionInstanceStatsData.getAvgProcessLatency();
                nonNullInstances++;
            }

            oneMin.setReceivedTotal(
                    oneMin.getReceivedTotal() + functionInstanceStatsData.getOneMin().getReceivedTotal()
            );
            oneMin.setProcessedSuccessfullyTotal(oneMin.getProcessedSuccessfullyTotal()
                    + functionInstanceStatsData.getOneMin().getProcessedSuccessfullyTotal());
            oneMin.setSystemExceptionsTotal(oneMin.getSystemExceptionsTotal()
                    + functionInstanceStatsData.getOneMin().getSystemExceptionsTotal());
            oneMin.setUserExceptionsTotal(oneMin.getUserExceptionsTotal()
                    + functionInstanceStatsData.getOneMin().getUserExceptionsTotal());
            if (functionInstanceStatsData.getOneMin().getAvgProcessLatency() != null) {
                if (oneMin.getAvgProcessLatency() == null) {
                    oneMin.setAvgProcessLatency(0.0);
                }
                oneMin.setAvgProcessLatency(oneMin.getAvgProcessLatency()
                        + functionInstanceStatsData.getOneMin().getAvgProcessLatency());
                nonNullInstancesOneMin++;
            }

            if (functionInstanceStatsData.getLastInvocation() != null) {
                if (lastInvocation == null || functionInstanceStatsData.getLastInvocation() > lastInvocation) {
                    lastInvocation = functionInstanceStatsData.getLastInvocation();
                }
            }
        }

        // calculate average from sum
        if (nonNullInstances > 0) {
            avgProcessLatency = avgProcessLatency / nonNullInstances;
        } else {
            avgProcessLatency = null;
        }

        // calculate 1min average from sum
        if (nonNullInstancesOneMin > 0) {
            oneMin.setAvgProcessLatency(oneMin.getAvgProcessLatency() / nonNullInstancesOneMin);
        } else {
            oneMin.setAvgProcessLatency(null);
        }

        return this;
    }

    public static FunctionStatsImpl decode (String json) throws IOException {
        return ObjectMapperFactory.getMapper().reader().readValue(json, FunctionStatsImpl.class);
    }
}
