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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Summary status of a single Pulsar Function, used by the batch
 * status-summary endpoint to avoid N+1 API calls.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FunctionStatusSummary {

    /**
     * Runtime state derived from instance counts.
     */
    public enum SummaryState {
        RUNNING,
        STOPPED,
        PARTIAL,
        UNKNOWN
    }

    /**
     * Classification of the status retrieval failure.
     */
    public enum ErrorType {
        AUTHENTICATION_FAILED,
        FUNCTION_NOT_FOUND,
        NETWORK_ERROR,
        INTERNAL_ERROR
    }

    private String name;
    private SummaryState state;
    private int numInstances;
    private int numRunning;

    /**
     * Non-null when the status query for this function failed;
     * the function will have {@code state = UNKNOWN} in that case.
     */
    private String error;
    private ErrorType errorType;
}
