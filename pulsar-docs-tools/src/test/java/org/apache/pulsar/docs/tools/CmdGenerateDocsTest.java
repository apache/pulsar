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
package org.apache.pulsar.docs.tools;

import static org.testng.Assert.assertEquals;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.testng.annotations.Test;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

public class CmdGenerateDocsTest {

    @Command
    public class Arguments {
        @Option(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-n", "--name"}, description = "Name")
        private String name;
    }

    @Test
    public void testHelp() {
        CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
        cmd.addCommand("test", new Arguments());
        StringWriter stringWriter = new StringWriter();
        cmd.getCommander().setOut(new PrintWriter(stringWriter));
        cmd.run(new String[]{"-h"});

        String message = stringWriter.toString();
        String rightMsg = "Usage: pulsar [-h] [-n=<commandNames>]... [COMMAND]\n"
                + "  -h, --help   Display help information\n"
                + "  -n, --command-names=<commandNames>\n"
                + "               List of command names\n"
                + "                 Default: []\n"
                + "Commands:\n"
                + "  test\n";
        assertEquals(message, rightMsg);
    }

    @Test
    public void testGenerateDocs() {
        CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
        cmd.addCommand("test", new Arguments());
        StringWriter stringWriter = new StringWriter();
        cmd.getCommander().setOut(new PrintWriter(stringWriter));
        cmd.run(null);

        String message = stringWriter.toString();
        String rightMsg = "# test\n\n"
                + "\n"
                + "\n"
                + "```shell\n"
                + "$ pulsar test options\n"
                + "```\n"
                + "\n"
                + "|Flag|Description|Default|\n"
                + "|---|---|---|\n"
                + "| `-h, --help` | Show this help message|false|\n"
                + "| `-n, --name` | Name|null|\n"
                + System.lineSeparator();
        assertEquals(message, rightMsg);
    }
}
