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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Function instance statistics data.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
        "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "userMetrics" })
public class FunctionInstanceStatsDataImpl
        extends FunctionInstanceStatsDataBaseImpl
        implements FunctionInstanceStatsData {
    @JsonProperty("1min")
    public FunctionInstanceStatsDataBaseImpl oneMin = new FunctionInstanceStatsDataBaseImpl();

    /**
     * Timestamp of when the function was last invoked for instance.
     **/
    public Long lastInvocation;

    /**
     * Map of user defined metrics.
     **/
    public Map<String, Double> userMetrics = new HashMap<>();
}
