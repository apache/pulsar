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
package org.apache.pulsar.admin.cli;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.google.common.collect.Sets;

@Parameters(commandDescription = "Operations about tenants")
public class CmdTenants extends CmdBase {
    @Parameters(commandDescription = "List the existing tenants")
    private class List extends CliCommand {
        @Override
        void run() throws PulsarAdminException {
            print(admin.tenants().getTenants());
        }
    }

    @Parameters(commandDescription = "Gets the configuration of a tenant")
    private class Get extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            print(admin.tenants().getTenantInfo(tenant));
        }
    }

    @Parameters(commandDescription = "Creates a new tenant")
    private class Create extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--admin-roles",
                "-r" }, description = "Comma separated Admin roles", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> adminRoles;

        @Parameter(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet(adminRoles), Sets.newHashSet(allowedClusters));
            admin.tenants().createTenant(tenant, tenantInfo);
        }
    }

    @Parameters(commandDescription = "Updates a tenant")
    private class Update extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--admin-roles",
                "-r" }, description = "Comma separated Admin roles", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> adminRoles;

        @Parameter(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet(adminRoles), Sets.newHashSet(allowedClusters));
            admin.tenants().updateTenant(tenant, tenantInfo);
        }
    }

    @Parameters(commandDescription = "Deletes an existing tenant")
    private class Delete extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            admin.tenants().deleteTenant(tenant);
        }
    }

    public CmdTenants(PulsarAdmin admin) {
        super("tenants", admin);
        jcommander.addCommand("list", new List());
        jcommander.addCommand("get", new Get());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("update", new Update());
        jcommander.addCommand("delete", new Delete());
    }

}
