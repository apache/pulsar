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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Authentication policies.
 */
public class AuthPolicies {
    @SuppressWarnings("checkstyle:MemberName")
    public final Map<String, Set<AuthAction>> namespace_auth;
    @SuppressWarnings("checkstyle:MemberName")
    public final Map<String, Map<String, Set<AuthAction>>> destination_auth;
    @SuppressWarnings("checkstyle:MemberName")
    public final Map<String, Set<String>> subscription_auth_roles;

    public AuthPolicies() {
        namespace_auth = Maps.newTreeMap();
        destination_auth = Maps.newTreeMap();
        subscription_auth_roles = Maps.newTreeMap();
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace_auth, destination_auth,
                subscription_auth_roles);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AuthPolicies) {
            AuthPolicies other = (AuthPolicies) obj;
            return Objects.equals(namespace_auth, other.namespace_auth)
                    && Objects.equals(destination_auth, other.destination_auth)
                    && Objects.equals(subscription_auth_roles, other.subscription_auth_roles);
        }

        return false;
    }
}
