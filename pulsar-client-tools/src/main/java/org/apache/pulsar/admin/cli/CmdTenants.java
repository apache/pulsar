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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.CommaParameterSplitter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

@Parameters(commandDescription = "Operations about tenants")
public class CmdTenants extends CmdBase {
    @Parameters(commandDescription = "List the existing tenants")
    private class List extends CliCommand {
        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().tenants().getTenants());
        }
    }

    @Parameters(commandDescription = "Gets the configuration of a tenant")
    private class Get extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            print(getAdmin().tenants().getTenantInfo(tenant));
        }
    }

    @Parameters(commandDescription = "Creates a new tenant")
    private class Create extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--admin-roles",
                "-r" }, description = "Comma separated list of auth principal allowed to administrate the tenant",
                required = false, splitter = CommaParameterSplitter.class)
        private java.util.List<String> adminRoles;

        @Parameter(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters. "
                + "If empty, the tenant will have access to all clusters",
                required = false, splitter = CommaParameterSplitter.class)
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);

            if (adminRoles == null) {
                adminRoles = Collections.emptyList();
            }

            if (allowedClusters == null || allowedClusters.isEmpty()) {
                // Default to all available cluster
                allowedClusters = getAdmin().clusters().getClusters();
            }

            TenantInfoImpl tenantInfo = new TenantInfoImpl(new HashSet<>(adminRoles), new HashSet<>(allowedClusters));
            getAdmin().tenants().createTenant(tenant, tenantInfo);
        }
    }

    @Parameters(commandDescription = "Updates the configuration for a tenant")
    private class Update extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--admin-roles",
                "-r" }, description = "Comma separated list of auth principal allowed to administrate the tenant. "
                + "If empty the current set of roles won't be modified",
                required = false, splitter = CommaParameterSplitter.class)
        private java.util.List<String> adminRoles;

        @Parameter(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters. "
                + "If omitted, the current set of clusters will be preserved",
                required = false, splitter = CommaParameterSplitter.class)
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);

            if (adminRoles == null) {
                adminRoles = new ArrayList<>(getAdmin().tenants().getTenantInfo(tenant).getAdminRoles());
            }

            if (allowedClusters == null) {
                allowedClusters = new ArrayList<>(getAdmin().tenants().getTenantInfo(tenant).getAllowedClusters());
            }

            TenantInfoImpl tenantInfo = new TenantInfoImpl(new HashSet<>(adminRoles), new HashSet<>(allowedClusters));
            getAdmin().tenants().updateTenant(tenant, tenantInfo);
        }
    }

    @Parameters(commandDescription = "Deletes an existing tenant")
    private class Delete extends CliCommand {
        @Parameter(description = "tenant-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
                "--force" }, description = "Delete a tenant forcefully by deleting all namespaces under it.")
        private boolean force = false;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            getAdmin().tenants().deleteTenant(tenant, force);
        }
    }

    public CmdTenants(Supplier<PulsarAdmin> admin) {
        super("tenants", admin);
        jcommander.addCommand("list", new List());
        jcommander.addCommand("get", new Get());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("update", new Update());
        jcommander.addCommand("delete", new Delete());
    }

    @Parameters(hidden = true)
    static class CmdProperties extends CmdTenants {
        public CmdProperties(Supplier<PulsarAdmin> admin) {
            super(admin);
        }

        @Override
        public boolean run(String[] args) {
            System.err.println("WARN: The properties subcommand is deprecated. Please use tenants instead");
            return super.run(args);
        }
    }
}
