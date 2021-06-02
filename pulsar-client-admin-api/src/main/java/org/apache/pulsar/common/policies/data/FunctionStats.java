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
package org.apache.pulsar.common.policies.data;

import java.util.List;

public interface FunctionStats {
    void addInstance(FunctionInstanceStats functionInstanceStats);

    FunctionStats calculateOverall();

    long getReceivedTotal();

    long getProcessedSuccessfullyTotal();

    long getSystemExceptionsTotal();

    long getUserExceptionsTotal();

    Double getAvgProcessLatency();

    FunctionInstanceStatsDataBase getOneMin();

    Long getLastInvocation();

    List<FunctionInstanceStats> getInstances();

    void setReceivedTotal(long receivedTotal);

    void setProcessedSuccessfullyTotal(long processedSuccessfullyTotal);

    void setSystemExceptionsTotal(long systemExceptionsTotal);

    void setUserExceptionsTotal(long userExceptionsTotal);

    void setAvgProcessLatency(Double avgProcessLatency);

    void setOneMin(FunctionInstanceStatsDataBase oneMin);

    void setLastInvocation(Long lastInvocation);

    void setInstances(List<FunctionInstanceStats> instances);
}
