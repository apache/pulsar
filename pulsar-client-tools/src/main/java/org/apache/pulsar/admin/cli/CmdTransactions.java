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

    public CmdTransactions(Supplier<PulsarAdmin> admin) {
        super("transactions", admin);
        jcommander.addCommand("coordinator-status", new GetCoordinatorStatus());
    }
}
