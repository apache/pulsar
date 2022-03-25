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
package org.apache.pulsar.client.admin.internal.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AuthPolicies;

@Data
@AllArgsConstructor
@NoArgsConstructor
public final class AuthPoliciesImpl implements AuthPolicies {

    @JsonProperty("namespace_auth")
    private Map<String, Set<AuthAction>> namespaceAuthentication = new TreeMap<>();

    @JsonProperty("destination_auth")
    private Map<String, Map<String, Set<AuthAction>>> topicAuthentication = new TreeMap<>();

    @JsonProperty("subscription_auth_roles")
    private Map<String, Set<String>> subscriptionAuthentication = new TreeMap<>();

    public static AuthPolicies.Builder builder() {
        return new AuthPoliciesImplBuilder();
    }


    public static class AuthPoliciesImplBuilder implements AuthPolicies.Builder {
        private Map<String, Set<AuthAction>> namespaceAuthentication = new TreeMap<>();
        private Map<String, Map<String, Set<AuthAction>>> topicAuthentication = new TreeMap<>();;
        private Map<String, Set<String>> subscriptionAuthentication = new TreeMap<>();;

        AuthPoliciesImplBuilder() {
        }

        public AuthPoliciesImplBuilder namespaceAuthentication(
                Map<String, Set<AuthAction>> namespaceAuthentication) {
            this.namespaceAuthentication = namespaceAuthentication;
            return this;
        }

        public AuthPoliciesImplBuilder topicAuthentication(
                Map<String, Map<String, Set<AuthAction>>> topicAuthentication) {
            this.topicAuthentication = topicAuthentication;
            return this;
        }

        public AuthPoliciesImplBuilder subscriptionAuthentication(
                Map<String, Set<String>> subscriptionAuthentication) {
            this.subscriptionAuthentication = subscriptionAuthentication;
            return this;
        }

        public AuthPoliciesImpl build() {
            return new AuthPoliciesImpl(namespaceAuthentication, topicAuthentication, subscriptionAuthentication);
        }

        public String toString() {
            return "AuthPoliciesImpl.AuthPoliciesImplBuilder(namespaceAuthentication=" + this.namespaceAuthentication
                    + ", topicAuthentication=" + this.topicAuthentication + ", subscriptionAuthentication="
                    + this.subscriptionAuthentication + ")";
        }
    }
}
