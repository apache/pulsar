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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The namespace isolation data for a given broker.
 */
@ApiModel(
        value = "BrokerNamespaceIsolationData",
        description = "The namespace isolation data for a given broker"
)
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class BrokerNamespaceIsolationDataImpl implements BrokerNamespaceIsolationData {

    @ApiModelProperty(
            name = "brokerName",
            value = "The broker name",
            example = "broker1:8080"
    )
    private String brokerName;
    @ApiModelProperty(
            name = "policyName",
            value = "Policy name",
            example = "my-policy"
    )
    private String policyName;
    @ApiModelProperty(
            name = "isPrimary",
            value = "Is Primary broker",
            example = "true/false"
    )
    private boolean isPrimary;
    @ApiModelProperty(
            name = "namespaceRegex",
            value = "The namespace-isolation policies attached to this broker"
    )
    private List<String> namespaceRegex; //isolated namespace regex

    public static BrokerNamespaceIsolationDataImplBuilder builder() {
        return new BrokerNamespaceIsolationDataImplBuilder();
    }


    public static class BrokerNamespaceIsolationDataImplBuilder implements BrokerNamespaceIsolationData.Builder {
        private String brokerName;
        private String policyName;
        private boolean isPrimary;
        private List<String> namespaceRegex;

        public BrokerNamespaceIsolationDataImplBuilder brokerName(String brokerName) {
            this.brokerName = brokerName;
            return this;
        }

        public BrokerNamespaceIsolationDataImplBuilder policyName(String policyName) {
            this.policyName = policyName;
            return this;
        }

        public BrokerNamespaceIsolationDataImplBuilder primary(boolean isPrimary) {
            this.isPrimary = isPrimary;
            return this;
        }

        public BrokerNamespaceIsolationDataImplBuilder namespaceRegex(List<String> namespaceRegex) {
            this.namespaceRegex = namespaceRegex;
            return this;
        }

        public BrokerNamespaceIsolationDataImpl build() {
            return new BrokerNamespaceIsolationDataImpl(brokerName, policyName, isPrimary, namespaceRegex);
        }
    }
}
