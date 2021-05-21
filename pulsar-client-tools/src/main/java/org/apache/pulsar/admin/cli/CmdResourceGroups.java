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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ResourceGroup;

import java.util.function.Supplier;

@Parameters(commandDescription = "Operations about ResourceGroups")
public class CmdResourceGroups extends CmdBase {
    @Parameters(commandDescription = "List the existing resourcegroups")
    private class List extends CliCommand {
        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().resourcegroups().getResourceGroups());
        }
    }

    @Parameters(commandDescription = "Gets the configuration of a resourcegroup")
    private class Get extends CliCommand {
        @Parameter(description = "resourcegroup-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String name = getOneArgument(params);
            print(getAdmin().resourcegroups().getResourceGroup(name));
        }
    }
    @Parameters(commandDescription = "Creates a new resourcegroup")
    private class Create extends CliCommand {
        @Parameter(description = "resourcegroup-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-publish-rate",
                "-mp" }, description = "message-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private int publishRateInMsgs = -1;

        @Parameter(names = { "--byte-publish-rate",
                "-bp" }, description = "byte-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private long publishRateInBytes = -1;


        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private int dispatchRateInMsgs = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long dispatchRateInBytes = -1;

        @Override
        void run() throws PulsarAdminException {
            String name = getOneArgument(params);

            ResourceGroup resourcegroup = new ResourceGroup();
            resourcegroup.setDispatchRateInMsgs(dispatchRateInMsgs);
            resourcegroup.setDispatchRateInBytes(dispatchRateInBytes);
            resourcegroup.setPublishRateInMsgs(publishRateInMsgs);
            resourcegroup.setPublishRateInBytes(publishRateInBytes);
            getAdmin().resourcegroups().createResourceGroup(name, resourcegroup);
        }
    }

    @Parameters(commandDescription = "Updates a resourcegroup")
    private class Update extends CliCommand {
        @Parameter(description = "resourcegroup-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-publish-rate",
                "-mp" }, description = "message-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private int publishRateInMsgs = -1;

        @Parameter(names = { "--byte-publish-rate",
                "-bp" }, description = "byte-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private long publishRateInBytes = -1;


        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private int dispatchRateInMsgs = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long dispatchRateInBytes = -1;

        @Override
        void run() throws PulsarAdminException {
            String name = getOneArgument(params);

            ResourceGroup resourcegroup = new ResourceGroup();
            resourcegroup.setDispatchRateInMsgs(dispatchRateInMsgs);
            resourcegroup.setDispatchRateInBytes(dispatchRateInBytes);
            resourcegroup.setPublishRateInMsgs(publishRateInMsgs);
            resourcegroup.setPublishRateInBytes(publishRateInBytes);

            getAdmin().resourcegroups().updateResourceGroup(name, resourcegroup);
        }
    }

    @Parameters(commandDescription = "Deletes an existing ResourceGroup")
    private class Delete extends CliCommand {
        @Parameter(description = "resourcegroup-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String name = getOneArgument(params);
            getAdmin().resourcegroups().deleteResourceGroup(name);
        }
    }


    public CmdResourceGroups(Supplier<PulsarAdmin> admin) {
        super("resourcegroups", admin);
        jcommander.addCommand("list", new CmdResourceGroups.List());
        jcommander.addCommand("get", new CmdResourceGroups.Get());
        jcommander.addCommand("create", new CmdResourceGroups.Create());
        jcommander.addCommand("update", new CmdResourceGroups.Update());
        jcommander.addCommand("delete", new CmdResourceGroups.Delete());
    }


}
