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

import com.google.common.base.Objects;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

/**
 * The namespace isolation data for a given broker.
 */
@ApiModel(
    value = "BrokerNamespaceIsolationData",
    description = "The namespace isolation data for a given broker"
)
public class BrokerNamespaceIsolationData {

    @ApiModelProperty(
        name = "brokerName",
        value = "The broker name",
        example = "broker1:8080"
    )
    public String brokerName;
    @ApiModelProperty(
            name = "policyName",
            value = "Policy name",
            example = "my-policy"
        )
    public String policyName;
    @ApiModelProperty(
            name = "isPrimary",
            value = "Is Primary broker",
            example = "true/false"
        )
    public boolean isPrimary;
    @ApiModelProperty(
        name = "namespaceRegex",
        value = "The namespace-isolation policies attached to this broker"
    )
    public List<String> namespaceRegex; //isolated namespace regex

    @Override
    public int hashCode() {
        return Objects.hashCode(brokerName, namespaceRegex);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BrokerNamespaceIsolationData) {
            BrokerNamespaceIsolationData other = (BrokerNamespaceIsolationData) obj;
            return Objects.equal(brokerName, other.brokerName) && Objects.equal(namespaceRegex, other.namespaceRegex);
        }
        return false;
    }

}
