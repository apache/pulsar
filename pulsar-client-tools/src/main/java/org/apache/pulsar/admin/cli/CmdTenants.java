/*
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about tenants")
public class CmdTenants extends CmdBase {
    @Command(description = "List the existing tenants")
    private class List extends CliCommand {
        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().tenants().getTenants());
        }
    }

    @Command(description = "Gets the configuration of a tenant")
    private class Get extends CliCommand {
        @Parameters(description = "tenant-name", arity = "1")
        private String tenant;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().tenants().getTenantInfo(tenant));
        }
    }

    @Command(description = "Creates a new tenant")
    private class Create extends CliCommand {
        @Parameters(description = "tenant-name", arity = "1")
        private String tenant;

        @Option(names = { "--admin-roles",
                "-r" }, description = "Comma separated list of auth principal allowed to administrate the tenant",
                required = false, split = ",")
        private java.util.List<String> adminRoles;

        @Option(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters. "
                + "If empty, the tenant will have access to all clusters",
                required = false, split = ",")
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
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

    @Command(description = "Updates the configuration for a tenant")
    private class Update extends CliCommand {
        @Parameters(description = "tenant-name", arity = "1")
        private String tenant;

        @Option(names = { "--admin-roles",
                "-r" }, description = "Comma separated list of auth principal allowed to administrate the tenant. "
                + "If empty the current set of roles won't be modified",
                required = false, split = ",")
        private java.util.List<String> adminRoles;

        @Option(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters. "
                + "If omitted, the current set of clusters will be preserved",
                required = false, split = ",")
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
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

    @Command(description = "Deletes an existing tenant")
    private class Delete extends CliCommand {
        @Parameters(description = "tenant-name", arity = "1")
        private String tenant;

        @Option(names = { "-f",
                "--force" }, description = "Delete a tenant forcefully by deleting all namespaces under it.")
        private boolean force = false;

        @Override
        void run() throws PulsarAdminException {
            getAdmin().tenants().deleteTenant(tenant, force);
        }
    }

    public CmdTenants(Supplier<PulsarAdmin> admin) {
        super("tenants", admin);
        addCommand("list", new List());
        addCommand("get", new Get());
        addCommand("create", new Create());
        addCommand("update", new Update());
        addCommand("delete", new Delete());
    }

    @Command(hidden = true)
    static class CmdProperties extends CmdTenants {
        public CmdProperties(Supplier<PulsarAdmin> admin) {
            super(admin);
        }
    }
}
