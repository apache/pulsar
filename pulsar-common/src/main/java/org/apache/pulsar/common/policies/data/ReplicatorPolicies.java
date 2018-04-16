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

import java.util.Map;

import com.google.common.base.Objects;

import lombok.ToString;

/**
 * External replicator policies for a namespace
 *
 */
@ToString
public class ReplicatorPolicies {

    public Map<String, String> replicationProperties;
    public String authParamStorePluginName; // auth plugin name using which provider can store/fetch credentials
    public Map<String, String> topicNameMapping;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ReplicatorPolicies) {
            ReplicatorPolicies other = (ReplicatorPolicies) obj;
            return Objects.equal(replicationProperties, other.replicationProperties)
                    && Objects.equal(authParamStorePluginName, other.authParamStorePluginName)
                    && Objects.equal(topicNameMapping, other.topicNameMapping);
        }
        return false;
    }

}
