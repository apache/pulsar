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

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.impl.AutoFailoverPolicyFactory;

/**
 * The auto failover policy configuration data.
 */
@ApiModel(
        value = "AutoFailoverPolicyData",
        description = "The auto failover policy configuration data"
)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AutoFailoverPolicyDataImpl implements AutoFailoverPolicyData {
    @ApiModelProperty(
            name = "policy_type",
            value = "The auto failover policy type",
            allowableValues = "min_available"
    )
    @JsonProperty("policy_type")
    private AutoFailoverPolicyType policyType;

    @ApiModelProperty(
            name = "parameters",
            value =
                    "The parameters applied to the auto failover policy specified by `policy_type`.\n"
                            + "The parameters for 'min_available' are :\n"
                            + "  - 'min_limit': the limit of minimal number of available brokers in primary"
                            + " group before auto failover\n"
                            + "  - 'usage_threshold': the resource usage threshold. If the usage of a broker"
                            + " is beyond this value, it would be marked as unavailable\n",
            example =
                    "{\n"
                            + "  \"min_limit\": 3,\n"
                            + "  \"usage_threshold\": 80\n"
                            + "}\n"
    )
    private Map<String, String> parameters;

    public static AutoFailoverPolicyDataImplBuilder builder() {
        return new AutoFailoverPolicyDataImplBuilder();
    }

    public void validate() {
        checkArgument(policyType != null && parameters != null);
        AutoFailoverPolicyFactory.create(this);
    }

    public static class AutoFailoverPolicyDataImplBuilder implements AutoFailoverPolicyData.Builder {
        private AutoFailoverPolicyType policyType;
        private Map<String, String> parameters;

        public AutoFailoverPolicyDataImplBuilder policyType(AutoFailoverPolicyType policyType) {
            this.policyType = policyType;
            return this;
        }

        public AutoFailoverPolicyDataImplBuilder parameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        public AutoFailoverPolicyDataImpl build() {
            return new AutoFailoverPolicyDataImpl(policyType, parameters);
        }
    }
}
