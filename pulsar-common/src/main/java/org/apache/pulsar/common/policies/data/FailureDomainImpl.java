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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import lombok.Data;
import lombok.ToString;

/**
 * The data of a failure domain configuration in a cluster.
 */
@ApiModel(
    value = "FailureDomain",
    description = "The data of a failure domain configuration in a cluster"
)
@ToString
@Data
public class FailureDomainImpl implements FailureDomain {

    @ApiModelProperty(
        name = "brokers",
        value = "The collection of brokers in the same failure domain",
        example = "[ 'broker-1', 'broker-2' ]"
    )
    public Set<String> brokers = new HashSet<String>();

    @Override
    public Set<String> getBrokers() {
        return brokers;
    }

    @Override
    public void setBrokers(Set<String> brokers) {
        this.brokers = brokers;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(brokers);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FailureDomainImpl) {
            FailureDomainImpl other = (FailureDomainImpl) obj;
            return Objects.equals(brokers, other.brokers);
        }

        return false;
    }
}
