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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.testng.annotations.Test;

public class CmdGenerateDocsTest {

    @Parameters(commandDescription = "Options")
    public class Arguments {
        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-n", "--name"}, description = "Name")
        private String name;
    }

    @Test
    public void testHelp() {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream(2048);
            PrintStream cacheStream = new PrintStream(baoStream);
            System.setOut(cacheStream);

            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("test", new Arguments());
            cmd.run(new String[]{"-h"});

            String message = baoStream.toString();
            String rightMsg = "Usage: pulsar gen-doc [options]\n"
                    + "  Options:\n"
                    + "    -n, --command-names\n"
                    + "      List of command names\n"
                    + "      Default: []\n"
                    + "    -h, --help\n"
                    + "      Display help information\n"
                    + "      Default: false\n"
                    + System.lineSeparator();
            assertEquals(rightMsg, message);
        } finally {
            System.setOut(oldStream);
        }
    }

    @Test
    public void testGenerateDocs() {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream(2048);
            PrintStream cacheStream = new PrintStream(baoStream);
            System.setOut(cacheStream);

            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("test", new Arguments());
            cmd.run(null);

            String message = baoStream.toString();
            String rightMsg = "------------\n"
                    + "\n"
                    + "# test\n"
                    + "\n"
                    + "### Usage\n"
                    + "\n"
                    + "`$test`\n"
                    + "\n"
                    + "------------\n"
                    + "\n"
                    + "Options\n"
                    + "\n"
                    + "\n"
                    + "```bdocs-tab:example_shell\n"
                    + "$ pulsar test options\n"
                    + "```\n"
                    + "\n"
                    + "|Flag|Description|Default|\n"
                    + "|---|---|---|\n"
                    + "| `-n, --name` | Name|null|\n"
                    + "| `-h, --help` | Show this help message|false|\n"
                    + System.lineSeparator();
            assertEquals(rightMsg, message);
        } finally {
            System.setOut(oldStream);
        }
    }
}
