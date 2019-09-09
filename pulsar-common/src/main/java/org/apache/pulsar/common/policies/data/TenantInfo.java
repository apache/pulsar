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

import com.google.common.collect.Sets;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Set;

import lombok.Data;

/**
 * Information of admin roles and allowed clusters for tenant.
 */
@Data
@ApiModel(value = "TenantInfo", description = "Information of adminRoles and allowedClusters for tenant")
public class TenantInfo {
    /**
     * List of role enabled as admin for this tenant.
     */
    @ApiModelProperty(
        value = "Comma separated list of auth principal allowed to administrate the tenant.",
        name = "adminRoles"
    )
    private Set<String> adminRoles;

    /**
     * List of clusters this tenant is restricted on.
     */
    @ApiModelProperty(
        value = "Comma separated allowed clusters.",
        name = "allowedClusters"
    )
    private Set<String> allowedClusters;

    public TenantInfo() {
        adminRoles = Sets.newHashSet();
        allowedClusters = Sets.newHashSet();
    }

    public TenantInfo(Set<String> adminRoles, Set<String> allowedClusters) {
        this.adminRoles = adminRoles;
        this.allowedClusters = allowedClusters;
    }

    public Set<String> getAdminRoles() {
        return adminRoles;
    }

    public void setAdminRoles(Set<String> adminRoles) {
        this.adminRoles = adminRoles;
    }

    public Set<String> getAllowedClusters() {
        return allowedClusters;
    }

    public void setAllowedClusters(Set<String> allowedClusters) {
        this.allowedClusters = allowedClusters;
    }
}
