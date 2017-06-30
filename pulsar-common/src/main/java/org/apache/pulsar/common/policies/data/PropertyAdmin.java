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

import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@XmlRootElement
public class PropertyAdmin {
    /**
     * List of role enabled as admin for this property
     */
    private List<String> adminRoles;

    /**
     * List of clusters this property is restricted on
     */
    private Set<String> allowedClusters;

    public PropertyAdmin() {
        adminRoles = Lists.newArrayList();
        allowedClusters = Sets.newHashSet();
    }

    public PropertyAdmin(List<String> adminRoles, Set<String> allowedClusters) {
        this.adminRoles = adminRoles;
        this.allowedClusters = allowedClusters;
    }

    public List<String> getAdminRoles() {
        return adminRoles;
    }

    public void setAdminRoles(List<String> adminRoles) {
        this.adminRoles = adminRoles;
    }

    public Set<String> getAllowedClusters() {
        return allowedClusters;
    }

    public void setAllowedClusters(Set<String> allowedClusters) {
        this.allowedClusters = allowedClusters;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PropertyAdmin) {
            PropertyAdmin other = (PropertyAdmin) obj;
            return Objects.equal(adminRoles, other.adminRoles) && Objects.equal(allowedClusters, other.allowedClusters);
        }

        return false;
    }

}
