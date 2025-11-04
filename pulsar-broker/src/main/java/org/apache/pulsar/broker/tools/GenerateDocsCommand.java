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
package org.apache.pulsar.broker.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The command to generate documents of broker-tool.
 */
@Command(name = "gen-doc", description = "Generate documents of broker-tool")
public class GenerateDocsCommand implements Callable<Integer> {
    @Option(
            names = {"-n", "--command-names"},
            description = "List of command names",
            arity = "0..1"
    )
    private List<String> commandNames = new ArrayList<>();
    private final CommandLine rootCmd;

    public GenerateDocsCommand(CommandLine rootCmd) {
        this.rootCmd = rootCmd;
    }

    @Override
    public Integer call() throws Exception {
        CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
        cmd.addCommand("broker-tool", rootCmd);
        if (commandNames.isEmpty()) {
            cmd.run(null);
        } else {
            ArrayList<String> args = new ArrayList(commandNames);
            args.add(0, "-n");
            cmd.run(args.toArray(new String[0]));
        }
        return 0;
    }
}
