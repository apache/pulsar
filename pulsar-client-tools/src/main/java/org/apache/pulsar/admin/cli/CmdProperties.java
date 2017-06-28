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
import org.apache.pulsar.common.policies.data.PropertyAdmin;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.google.common.collect.Sets;

@Parameters(commandDescription = "Operations about properties")
public class CmdProperties extends CmdBase {
    @Parameters(commandDescription = "List the existing properties")
    private class List extends CliCommand {
        @Override
        void run() throws PulsarAdminException {
            print(admin.properties().getProperties());
        }
    }

    @Parameters(commandDescription = "Gets the configuration of a property")
    private class Get extends CliCommand {
        @Parameter(description = "property-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String property = getOneArgument(params);
            print(admin.properties().getPropertyAdmin(property));
        }
    }

    @Parameters(commandDescription = "Creates a new property")
    private class Create extends CliCommand {
        @Parameter(description = "property-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--admin-roles",
                "-r" }, description = "Comma separated Admin roles", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> adminRoles;

        @Parameter(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
            String property = getOneArgument(params);
            PropertyAdmin propertyAdmin = new PropertyAdmin(adminRoles, Sets.newHashSet(allowedClusters));
            admin.properties().createProperty(property, propertyAdmin);
        }
    }

    @Parameters(commandDescription = "Updates a property")
    private class Update extends CliCommand {
        @Parameter(description = "property-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--admin-roles",
                "-r" }, description = "Comma separated Admin roles", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> adminRoles;

        @Parameter(names = { "--allowed-clusters",
                "-c" }, description = "Comma separated allowed clusters", required = true, splitter = CommaParameterSplitter.class)
        private java.util.List<String> allowedClusters;

        @Override
        void run() throws PulsarAdminException {
            String property = getOneArgument(params);
            PropertyAdmin propertyAdmin = new PropertyAdmin(adminRoles, Sets.newHashSet(allowedClusters));
            admin.properties().updateProperty(property, propertyAdmin);
        }
    }

    @Parameters(commandDescription = "Deletes an existing property")
    private class Delete extends CliCommand {
        @Parameter(description = "property-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String property = getOneArgument(params);
            admin.properties().deleteProperty(property);
        }
    }

    public CmdProperties(PulsarAdmin admin) {
        super("properties", admin);
        jcommander.addCommand("list", new List());
        jcommander.addCommand("get", new Get());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("update", new Update());
        jcommander.addCommand("delete", new Delete());
    }

}
