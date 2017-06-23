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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

public class NamespaceIsolationData {

    public List<String> namespaces = new ArrayList<String>();
    public List<String> primary = new ArrayList<String>();
    public List<String> secondary = new ArrayList<String>();
    public AutoFailoverPolicyData auto_failover_policy;

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
