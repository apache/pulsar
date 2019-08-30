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

import com.google.common.base.Objects;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import org.apache.pulsar.common.policies.impl.AutoFailoverPolicyFactory;

/**
 * The auto failover policy configuration data.
 */
@ApiModel(
    value = "AutoFailoverPolicyData",
    description = "The auto failover policy configuration data"
)
public class AutoFailoverPolicyData {
    @ApiModelProperty(
        name = "policy_type",
        value = "The auto failover policy type",
        allowableValues = "min_available"
    )
    @SuppressWarnings("checkstyle:MemberName")
    public AutoFailoverPolicyType policy_type;
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
    public Map<String, String> parameters;

    @Override
    public int hashCode() {
        return Objects.hashCode(policy_type, parameters);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AutoFailoverPolicyData) {
            AutoFailoverPolicyData other = (AutoFailoverPolicyData) obj;
            return Objects.equal(policy_type, other.policy_type) && Objects.equal(parameters, other.parameters);
        }

        return false;
    }

    public void validate() {
        checkArgument(policy_type != null && parameters != null);
        AutoFailoverPolicyFactory.create(this);
    }

    @Override
    public String toString() {
        return String.format("policy_type=%s parameters=%s", policy_type, parameters);
    }
}
