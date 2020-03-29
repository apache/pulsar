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
import java.util.ArrayList;
import java.util.List;

/**
 * The data of namespace isolation configuration.
 */
@ApiModel(
    value = "NamespaceIsolationData",
    description = "The data of namespace isolation configuration"
)
public class NamespaceIsolationData {

    @ApiModelProperty(
        name = "namespaces",
        value = "The list of namespaces to apply this namespace isolation data"
    )
    public List<String> namespaces = new ArrayList<String>();
    @ApiModelProperty(
        name = "primary",
        value = "The list of primary brokers for serving the list of namespaces in this isolation policy"
    )
    public List<String> primary = new ArrayList<String>();
    @ApiModelProperty(
        name = "primary",
        value = "The list of secondary brokers for serving the list of namespaces in this isolation policy"
    )
    public List<String> secondary = new ArrayList<String>();
    @ApiModelProperty(
        name = "auto_failover_policy",
        value = "The data of auto-failover policy configuration",
        example =
              "{"
            + "  \"policy_type\": \"min_available\""
            + "  \"parameters\": {"
            + "    \"\": \"\""
            + "  }"
            + "}"
    )
    @SuppressWarnings("checkstyle:MemberName")
    public AutoFailoverPolicyData auto_failover_policy;

    @Override
    public int hashCode() {
        return Objects.hashCode(namespaces, primary, secondary,
                auto_failover_policy);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NamespaceIsolationData) {
            NamespaceIsolationData other = (NamespaceIsolationData) obj;
            return Objects.equal(namespaces, other.namespaces) && Objects.equal(primary, other.primary)
                    && Objects.equal(secondary, other.secondary)
                    && Objects.equal(auto_failover_policy, other.auto_failover_policy);
        }

        return false;
    }

    public void validate() {
        checkArgument(namespaces != null && !namespaces.isEmpty() && primary != null && !primary.isEmpty()
                && secondary != null && auto_failover_policy != null);
        auto_failover_policy.validate();
    }

    @Override
    public String toString() {
        return String.format("namespaces=%s primary=%s secondary=%s auto_failover_policy=%s", namespaces, primary,
                secondary, auto_failover_policy);
    }
}
