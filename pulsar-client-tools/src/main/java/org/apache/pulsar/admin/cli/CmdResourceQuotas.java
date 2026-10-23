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
import org.apache.pulsar.common.policies.data.ResourceQuota;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(description = "Operations about resource quotas")
public class CmdResourceQuotas extends CmdBase {

    @Command(description = "Get the resource quota for specified namespace bundle, "
            + "or default quota if no namespace/bundle specified.")
    private class GetResourceQuota extends CliCommand {

        @Option(names = { "--namespace",
                "-n" }, description = "tenant/namespace, must be specified together with '--bundle'")
        private String namespaceName;

        @Option(names = { "--bundle",
                "-b" }, description = "{start-boundary}_{end-boundary}, must be specified together with '--namespace'")
        private String bundle;

        @Override
        void run() throws PulsarAdminException, ParameterException {
            if (bundle == null && namespaceName == null) {
                print(getAdmin().resourceQuotas().getDefaultResourceQuota());
            } else if (bundle != null && namespaceName != null) {
                String namespace = validateNamespace(namespaceName);
                print(getAdmin().resourceQuotas().getNamespaceBundleResourceQuota(namespace, bundle));
            } else {
                throw new ParameterException("namespace and bundle must be provided together.");
            }
        }
    }

    @Command(description = "Set the resource quota for specified namespace bundle, "
            + "or default quota if no namespace/bundle specified.")
    private class SetResourceQuota extends CliCommand {

        @Option(names = { "--namespace",
                "-n" }, description = "tenant/namespace, must be specified together with '--bundle'")
        private String namespaceName;

        @Option(names = { "--bundle",
                "-b" }, description = "{start-boundary}_{end-boundary}, must be specified together with '--namespace'")
        private String bundle;

        @Option(names = { "--msgRateIn",
                "-mi" }, description = "expected incoming messages per second", required = true)
        private long msgRateIn = 0;

        @Option(names = { "--msgRateOut",
                "-mo" }, description = "expected outgoing messages per second", required = true)
        private long msgRateOut = 0;

        @Option(names = {"--bandwidthIn",
                "-bi"}, description = "expected inbound bandwidth (bytes/second)", required = true)
        private long bandwidthIn = 0;

        @Option(names = { "--bandwidthOut",
                "-bo" }, description = "expected outbound bandwidth (bytes/second)", required = true)
        private long bandwidthOut = 0;

        @Option(names = { "--memory", "-mem" }, description = "expected memory usage (Mbytes)", required = true)
        private long memory = 0;

        @Option(names = { "--dynamic",
                "-d" }, description = "dynamic (allow to be dynamically re-calculated) or not")
        private boolean dynamic = false;

        @Override
        void run() throws PulsarAdminException {
            ResourceQuota quota = new ResourceQuota();
            quota.setMsgRateIn(msgRateIn);
            quota.setMsgRateOut(msgRateOut);
            quota.setBandwidthIn(bandwidthIn);
            quota.setBandwidthOut(bandwidthOut);
            quota.setMemory(memory);
            quota.setDynamic(dynamic);

            if (bundle == null && namespaceName == null) {
                getAdmin().resourceQuotas().setDefaultResourceQuota(quota);
            } else if (bundle != null && namespaceName != null) {
                String namespace = validateNamespace(namespaceName);
                getAdmin().resourceQuotas().setNamespaceBundleResourceQuota(namespace, bundle, quota);
            } else {
                throw new ParameterException("namespace and bundle must be provided together.");
            }
        }
    }

    @Command(description = "Reset the specified namespace bundle's resource quota to default value.")
    private class ResetNamespaceBundleResourceQuota extends CliCommand {

        @Option(names = { "--namespace", "-n" }, description = "tenant/namespace", required = true)
        private String namespaceName;

        @Option(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}", required = true)
        private String bundle;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().resourceQuotas().resetNamespaceBundleResourceQuota(namespace, bundle);
        }
    }

    public CmdResourceQuotas(Supplier<PulsarAdmin> admin) {
        super("resource-quotas", admin);
        addCommand("get", new GetResourceQuota());
        addCommand("set", new SetResourceQuota());
        addCommand("reset-namespace-bundle-quota", new ResetNamespaceBundleResourceQuota());
    }
}
