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
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

public class AuthPolicies {
    public final Map<String, Set<AuthAction>> namespace_auth;
    public final Map<String, Map<String, Set<AuthAction>>> destination_auth;

    public AuthPolicies() {
        namespace_auth = Maps.newTreeMap();
        destination_auth = Maps.newTreeMap();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AuthPolicies) {
            AuthPolicies other = (AuthPolicies) obj;
            return Objects.equal(namespace_auth, other.namespace_auth)
                    && Objects.equal(destination_auth, other.destination_auth);
        }

        return false;
    }
}
