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
package org.apache.pulsar.broker.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.pulsar.common.util.CmdGenerateDocs;

/**
 * The command to generate documents of broker-tool.
 */
public class GenerateDocsCommand extends CliCommand<CliFlags, GenerateDocsCommand.GenDocFlags> {

    private static final String NAME = "gen-doc";
    private static final String DESC = "Generate documents of broker-tool";

    /**
     * The CLI flags of gen docs command.
     */
    protected static class GenDocFlags extends CliFlags {
        @Parameter(
                names = {"-n", "--command-names"},
                description = "List of command names"
        )
        private List<String> commandNames = new ArrayList<>();
    }

    public GenerateDocsCommand() {
        super(CliSpec.<GenDocFlags>newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(new GenDocFlags())
                .build());
    }

    @Override
    public Boolean apply(CliFlags globalFlags, String[] args) {
        CliSpec<GenDocFlags> newSpec = CliSpec.newBuilder(spec)
                .withRunFunc(cmdFlags -> apply(cmdFlags))
                .build();
        return 0 == Cli.runCli(newSpec, args);
    }

    private boolean apply(GenerateDocsCommand.GenDocFlags flags) {
        CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
        JCommander commander = new JCommander();
        commander.addCommand("load-report", new LoadReportCommand.Flags());
        cmd.addCommand("broker-tool", commander);
        if (flags.commandNames.isEmpty()) {
            cmd.run(null);
        } else {
            ArrayList<String> args = new ArrayList(flags.commandNames);
            args.add(0, "-n");
            cmd.run(args.toArray(new String[0]));
        }
        return true;
    }
}
