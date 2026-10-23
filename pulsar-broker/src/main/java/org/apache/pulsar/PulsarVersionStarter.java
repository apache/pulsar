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
package org.apache.pulsar;

import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * Pulsar version entry point.
 */
public class PulsarVersionStarter {

    @Command(name = "version", showDefaultValues = true, scope = ScopeType.INHERIT)
    private static class Arguments {
        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void main(String[] args) {
        Arguments arguments = new Arguments();
        CommandLine commander = new CommandLine(arguments);
        try {
            commander.parseArgs(args);
            if (arguments.help) {
                commander.usage(commander.getOut());
                return;
            }
            if (arguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("version", commander);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            commander.getErr().println(e);
            return;
        }
        System.out.println("Current version of pulsar is: " + PulsarVersion.getVersion());
        System.out.println("Git Revision " + PulsarVersion.getGitSha());
        System.out.println("Git Branch " + PulsarVersion.getGitBranch());
        System.out.println("Built by " + PulsarVersion.getBuildUser() + " on " + PulsarVersion.getBuildHost() + " at "
                + PulsarVersion.getBuildTime());
    }
}
