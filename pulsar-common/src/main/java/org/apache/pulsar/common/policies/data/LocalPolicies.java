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

import static org.apache.pulsar.common.policies.data.Policies.defaultBundle;
import com.google.common.base.Objects;

/**
 * Local policies.
 */
public class LocalPolicies {

    public final BundlesData bundles;
    // bookie affinity group for bookie-isolation
    public final BookieAffinityGroupData bookieAffinityGroup;
    // namespace anti-affinity-group
    public final String namespaceAntiAffinityGroup;

    public LocalPolicies() {
        bundles = defaultBundle();
        bookieAffinityGroup = null;
        namespaceAntiAffinityGroup = "";
    }

    public LocalPolicies(BundlesData data,BookieAffinityGroupData bookieAffinityGroup,String namespaceAntiAffinityGroup) {
        bundles = data;
        this.bookieAffinityGroup = bookieAffinityGroup;
        this.namespaceAntiAffinityGroup = namespaceAntiAffinityGroup;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bundles, bookieAffinityGroup);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LocalPolicies) {
            LocalPolicies other = (LocalPolicies) obj;
            return Objects.equal(bundles, other.bundles)
                    && Objects.equal(bookieAffinityGroup, other.bookieAffinityGroup)
                    && Objects.equal(namespaceAntiAffinityGroup, other.namespaceAntiAffinityGroup);
        }
        return false;
    }

    @Override
    public String toString() {
        return "LocalPolicies{" +
                "bundles=" + bundles +
                ", bookieAffinityGroup=" + bookieAffinityGroup +
                ", namespaceAntiAffinityGroup='" + namespaceAntiAffinityGroup + '\'' +
                '}';
    }
}
