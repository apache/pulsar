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
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.transaction.TxnID;

@Parameters(commandDescription = "Operations on transactions")
public class CmdTransactions extends CmdBase {

    @Parameters(commandDescription = "Get transaction coordinator status")
    private class GetCoordinatorStatus extends CliCommand {
        @Parameter(names = {"-c", "--coordinator-id"}, description = "the coordinator id", required = false)
        private Integer coordinatorId;

        @Override
        void run() throws Exception {
            if (coordinatorId != null) {
                print(getAdmin().transactions().getCoordinatorStatusById(coordinatorId));
            } else {
                print(getAdmin().transactions().getCoordinatorStatusList());
            }
        }
    }

    @Parameters(commandDescription = "Get transaction in pending ack stats")
    private class GetTransactionInPendingAckStats extends CliCommand {
        @Parameter(names = {"-c", "--coordinator-id"}, description = "the coordinator id", required = true)
        private int coordinatorId;

        @Parameter(names = {"-id", "--sequencce-id"}, description = "the sequenceId", required = true)
        private int sequenceId;

        @Parameter(names = {"-t", "--topic"}, description = "the topic name", required = true)
        private String topic;

        @Parameter(names = {"-s", "--sub-name"}, description = "the subscription name", required = true)
        private String subName;

        @Override
        void run() throws Exception {
            getAdmin().transactions().getTransactionInPendingAckStats(new TxnID(coordinatorId, sequenceId),
                    topic, subName);
        }
    }

    public CmdTransactions(Supplier<PulsarAdmin> admin) {
        super("transactions", admin);
        jcommander.addCommand("coordinator-status", new GetCoordinatorStatus());
        jcommander.addCommand("transaction-in-pending-ack-stats", new GetTransactionInPendingAckStats());
    }
}
