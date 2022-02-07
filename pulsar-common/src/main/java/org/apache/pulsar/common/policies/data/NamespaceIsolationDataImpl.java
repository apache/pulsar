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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * The data of namespace isolation configuration.
 */
@ApiModel(
        value = "NamespaceIsolationData",
        description = "The data of namespace isolation configuration"
)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NamespaceIsolationDataImpl implements NamespaceIsolationData {

    @ApiModelProperty(
            name = "namespaces",
            value = "The list of namespaces to apply this namespace isolation data"
    )
    private List<String> namespaces;

    @ApiModelProperty(
            name = "primary",
            value = "The list of primary brokers for serving the list of namespaces in this isolation policy"
    )
    private List<String> primary;

    @ApiModelProperty(
            name = "secondary",
            value = "The list of secondary brokers for serving the list of namespaces in this isolation policy"
    )
    private List<String> secondary;

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
    @JsonProperty("auto_failover_policy")
    private AutoFailoverPolicyData autoFailoverPolicy;

    public static NamespaceIsolationDataImplBuilder builder() {
        return new NamespaceIsolationDataImplBuilder();
    }

    public void validate() {
        checkArgument(namespaces != null && !namespaces.isEmpty() && primary != null && !primary.isEmpty()
                && validateRegex(primary) && secondary != null && validateRegex(secondary)
                && autoFailoverPolicy != null);
        autoFailoverPolicy.validate();
    }

    private boolean validateRegex(List<String> policies) {
        if (policies != null && !policies.isEmpty()) {
            policies.forEach((policy) -> {
                try {
                    if (StringUtils.isNotBlank(policy)) {
                        Pattern.compile(policy);
                    }
                } catch (PatternSyntaxException exception) {
                    throw new IllegalArgumentException("invalid policy regex " + policy);
                }
            });
        }
        return true;
    }

    public static class NamespaceIsolationDataImplBuilder implements NamespaceIsolationData.Builder {
        private List<String> namespaces = new ArrayList<>();
        private List<String> primary = new ArrayList<>();
        private List<String> secondary = new ArrayList<>();
        private AutoFailoverPolicyData autoFailoverPolicy;

        public NamespaceIsolationDataImplBuilder namespaces(List<String> namespaces) {
            this.namespaces = namespaces;
            return this;
        }

        public NamespaceIsolationDataImplBuilder primary(List<String> primary) {
            this.primary = primary;
            return this;
        }

        public NamespaceIsolationDataImplBuilder secondary(List<String> secondary) {
            this.secondary = secondary;
            return this;
        }

        public NamespaceIsolationDataImplBuilder autoFailoverPolicy(AutoFailoverPolicyData autoFailoverPolicy) {
            this.autoFailoverPolicy = autoFailoverPolicy;
            return this;
        }

        public NamespaceIsolationDataImpl build() {
            return new NamespaceIsolationDataImpl(namespaces, primary, secondary, autoFailoverPolicy);
        }
    }
}
