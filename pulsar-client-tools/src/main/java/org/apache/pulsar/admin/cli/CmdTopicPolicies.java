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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations on persistent topics")
public class CmdTopicPolicies extends CmdBase {

    public CmdTopicPolicies(Supplier<PulsarAdmin> admin) {
        super("topicPolicies", admin);

        jcommander.addCommand("get-retention", new GetRetention());
        jcommander.addCommand("set-retention", new SetRetention());
        jcommander.addCommand("remove-retention", new RemoveRetention());
    }

    @Parameters(commandDescription = "Get the retention policy for a topic")
    private class GetRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Parameter(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies", arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopicPolicies(isGlobal).getRetention(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set the retention policy for a topic")
    private class SetRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private List<String> params;

        @Parameter(names = { "--time",
                "-t" }, description = "Retention time in minutes (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w). "
                + "0 means no retention and -1 means infinite time retention", required = true)
        private String retentionTimeStr;

        @Parameter(names = { "--size", "-s" }, description = "Retention size limit (eg: 10M, 16G, 3T). "
                + "0 or less than 1MB means no retention and -1 means infinite size retention", required = true)
        private String limitStr;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously", arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            long sizeLimit = validateSizeString(limitStr);
            long retentionTimeInSec = RelativeTimeUtil.parseRelativeTimeInSeconds(retentionTimeStr);

            final int retentionTimeInMin;
            if (retentionTimeInSec != -1) {
                retentionTimeInMin = (int) TimeUnit.SECONDS.toMinutes(retentionTimeInSec);
            } else {
                retentionTimeInMin = -1;
            }

            final int retentionSizeInMB;
            if (sizeLimit != -1) {
                retentionSizeInMB = (int) (sizeLimit / (1024 * 1024));
            } else {
                retentionSizeInMB = -1;
            }
            getTopicPolicies(isGlobal).setRetention(persistentTopic,
                    new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Parameters(commandDescription = "Remove the retention policy for a topic")
    private class RemoveRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private List<String> params;

        @Parameter(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously"
                , arity = 0)
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopicPolicies(isGlobal).removeRetention(persistentTopic);
        }
    }

    private TopicPolicies getTopicPolicies(boolean isGlobal) {
        return getAdmin().topicPolicies(isGlobal);
    }

}
