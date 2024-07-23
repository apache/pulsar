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

import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about ResourceGroups")
public class CmdResourceGroups extends CmdBase {
    @Command(description = "List the existing resourcegroups")
    private class List extends CliCommand {
        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().resourcegroups().getResourceGroups());
        }
    }

    @Command(description = "Gets the configuration of a resourcegroup")
    private class Get extends CliCommand {
        @Parameters(description = "resourcegroup-name", arity = "1")
        private String resourceGroupName;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().resourcegroups().getResourceGroup(resourceGroupName));
        }
    }

    @Command(description = "Creates a new resourcegroup")
    private class Create extends CliCommand {
        @Parameters(description = "resourcegroup-name", arity = "1")
        private String resourceGroupName;

        @Option(names = { "--msg-publish-rate",
                "-mp" }, description = "message-publish-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private Integer publishRateInMsgs;

        @Option(names = { "--byte-publish-rate",
                "-bp" }, description = "byte-publish-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private Long publishRateInBytes;


        @Option(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private Integer dispatchRateInMsgs;

        @Option(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private Long dispatchRateInBytes;

        @Override
        void run() throws PulsarAdminException {
            ResourceGroup resourcegroup = new ResourceGroup();
            resourcegroup.setDispatchRateInMsgs(dispatchRateInMsgs);
            resourcegroup.setDispatchRateInBytes(dispatchRateInBytes);
            resourcegroup.setPublishRateInMsgs(publishRateInMsgs);
            resourcegroup.setPublishRateInBytes(publishRateInBytes);
            getAdmin().resourcegroups().createResourceGroup(resourceGroupName, resourcegroup);
        }
    }

    @Command(description = "Updates a resourcegroup")
    private class Update extends CliCommand {
        @Parameters(description = "resourcegroup-name", arity = "1")
        private String resourceGroupName;

        @Option(names = { "--msg-publish-rate",
                "-mp" }, description = "message-publish-rate ", required = false)
        private Integer publishRateInMsgs;

        @Option(names = { "--byte-publish-rate",
                "-bp" }, description = "byte-publish-rate ", required = false)
        private Long publishRateInBytes;


        @Option(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate ", required = false)
        private Integer dispatchRateInMsgs;

        @Option(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate ", required = false)
        private Long dispatchRateInBytes;

        @Override
        void run() throws PulsarAdminException {
            ResourceGroup resourcegroup = new ResourceGroup();
            resourcegroup.setDispatchRateInMsgs(dispatchRateInMsgs);
            resourcegroup.setDispatchRateInBytes(dispatchRateInBytes);
            resourcegroup.setPublishRateInMsgs(publishRateInMsgs);
            resourcegroup.setPublishRateInBytes(publishRateInBytes);

            getAdmin().resourcegroups().updateResourceGroup(resourceGroupName, resourcegroup);
        }
    }

    @Command(description = "Deletes an existing ResourceGroup")
    private class Delete extends CliCommand {
        @Parameters(description = "resourcegroup-name", arity = "1")
        private String resourceGroupName;

        @Override
        void run() throws PulsarAdminException {
            getAdmin().resourcegroups().deleteResourceGroup(resourceGroupName);
        }
    }


    public CmdResourceGroups(Supplier<PulsarAdmin> admin) {
        super("resourcegroups", admin);
        addCommand("list", new CmdResourceGroups.List());
        addCommand("get", new CmdResourceGroups.Get());
        addCommand("create", new CmdResourceGroups.Create());
        addCommand("update", new CmdResourceGroups.Update());
        addCommand("delete", new CmdResourceGroups.Delete());
    }


}
