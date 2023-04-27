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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Strings;
import java.util.function.Supplier;
import lombok.NonNull;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.BookieInfo;

@Parameters(commandDescription = "Operations about bookies rack placement")
public class CmdBookies extends CmdBase {

    @Parameters(commandDescription = "Gets the rack placement information for all the bookies in the cluster")
    private class GetAll extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().bookies().getBookiesRackInfo());
        }
    }

    @Parameters(commandDescription = "Gets the rack placement information for a specific bookie in the cluster")
    private class GetBookie extends CliCommand {

        @Parameter(names = { "-b", "--bookie" },
                description = "Bookie address (format: `address:port`)", required = true)
        private String bookieAddress;

        @Override
        void run() throws Exception {
            print(getAdmin().bookies().getBookieRackInfo(bookieAddress));
        }
    }

    @Parameters(commandDescription = "List bookies")
    private class ListBookies extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().bookies().getBookies());
        }
    }

    @Parameters(commandDescription = "Remove rack placement information for a specific bookie in the cluster")
    private class RemoveBookie extends CliCommand {

        @Parameter(names = { "-b", "--bookie" },
                description = "Bookie address (format: `address:port`)", required = true)
        private String bookieAddress;

        @Override
        void run() throws Exception {
            getAdmin().bookies().deleteBookieRackInfo(bookieAddress);
        }
    }

    @Parameters(commandDescription = "Updates the rack placement information for a specific bookie in the cluster "
            + "(note. bookie address format:`address:port`)")
    private class UpdateBookie extends CliCommand {
        private static final String PATH_SEPARATOR = "/";

        @Parameter(names = { "-g", "--group" }, description = "Bookie group name", required = false)
        private String group = "default";

        @Parameter(names = { "-b", "--bookie" },
                description = "Bookie address (format: `address:port`)", required = true)
        private String bookieAddress;

        @Parameter(names = { "-r", "--rack" }, description = "Bookie rack name. "
                + "If you set a bookie rack name to slash (/) "
                + "or an empty string (\"\"): "
                + "when using Pulsar earlier than 2.7.5, 2.8.3, and 2.9.2, "
                + "an exception is thrown; "
                + "if you use Pulsar 2.7.5, 2.8.3, 2.9.2 or later versions, "
                + "it falls back to /default-rack or /default-region/default-rack."
                + "When `RackawareEnsemblePlacementPolicy` is enabled, "
                + "the rack name is not allowed to contain slash (/) "
                + "except for the beginning and end of the rack name string. "
                + "For example, rack name like /rack0 is okay, but /rack/0 is not allowed. "
                + "When `RegionawareEnsemblePlacementPolicy` is enabled, "
                + "the rack name can only contain one slash (/) "
                + "except for the beginning and end of the rack name string. "
                + "For example, rack name like /region0/rack0 is okay, "
                + "but /region0rack0 and /region0/rack/0 are not allowed.", required = true)
        private String bookieRack;

        @Parameter(names = {"-hn", "--hostname"}, description = "Bookie host name", required = false)
        private String bookieHost;

        @Override
        void run() throws Exception {
            checkArgument(!Strings.isNullOrEmpty(bookieRack) && !bookieRack.trim().equals(PATH_SEPARATOR),
                    "rack name is invalid, it should not be null, empty or '/'");

            getAdmin().bookies().updateBookieRackInfo(bookieAddress, group,
                    BookieInfo.builder()
                            .rack(bookieRack)
                            .hostname(bookieHost)
                            .build());
        }

        private void checkArgument(boolean expression, @NonNull Object errorMessage) {
            if (!expression) {
                throw new ParameterException(String.valueOf(errorMessage));
            }
        }
    }

    public CmdBookies(Supplier<PulsarAdmin> admin) {
        super("bookies", admin);
        jcommander.addCommand("racks-placement", new GetAll());
        jcommander.addCommand("list-bookies", new ListBookies());
        jcommander.addCommand("get-bookie-rack", new GetBookie());
        jcommander.addCommand("delete-bookie-rack", new RemoveBookie());
        jcommander.addCommand("set-bookie-rack", new UpdateBookie());
    }
}
