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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

/**
 * Function instance statistics data base.
 */
@Data
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
        "userExceptionsTotal", "avgProcessLatency" })
public class FunctionInstanceStatsDataBaseImpl implements FunctionInstanceStatsDataBase {
    /**
     * Total number of records function received from source for instance.
     **/
    public long receivedTotal;

    /**
     * Total number of records successfully processed by user function for instance.
     **/
    public long processedSuccessfullyTotal;

    /**
     * Total number of system exceptions thrown for instance.
     **/
    public long systemExceptionsTotal;

    /**
     * Total number of user exceptions thrown for instance.
     **/
    public long userExceptionsTotal;

    /**
     * Average process latency for function for instance.
     **/
    public Double avgProcessLatency;
}
