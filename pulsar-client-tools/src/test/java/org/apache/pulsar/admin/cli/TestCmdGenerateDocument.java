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

import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.testng.annotations.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

public class TestCmdGenerateDocument {
    private String generate(Consumer<CommandLine> action) {
        PrintStream original = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            System.setOut(output);
            CommandLine root = new CommandLine(CommandSpec.create().name("pulsar-admin"));
            root.addSubcommand("brokers", new CmdBrokers(() -> null).getCommander(), "broker-alias");
            root.addSubcommand("ns-isolation-policy", new CmdNamespaceIsolationPolicy(() -> null).getCommander());
            root.addSubcommand("documents", new CmdGenerateDocument(() -> null).getCommander());
            action.accept(root);
            return bytes.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(original);
        }
    }

    @Test
    public void testEachSubcommandIsPrintedOnce() {
        String document = generate(root -> assertThat(root.execute("documents", "generate", "brokers")).isZero());
        assertThat(document.lines().filter(line -> line.equals("# brokers")).count()).isEqualTo(1);
        assertThat(document).containsOnlyOnce("## list\n");
        assertThat(document).containsOnlyOnce("## shutdown\n");
    }

    @Test
    public void testSubcommandOptionsAndTableColumns() {
        String document = generate(root ->
                assertThat(root.execute("documents", "generate", "ns-isolation-policy")).isZero());
        assertThat(document).contains("--primary", "--secondary", "--unload-scope");
        assertThat(document.lines().filter(line -> line.startsWith("|")))
                .allSatisfy(line -> assertThat(line.chars().filter(c -> c == '|').count()).isEqualTo(4));
    }

    @Test
    public void testMultipleModulesAndAliases() {
        String document = generate(root -> assertThat(root.execute("documents", "generate",
                "brokers", "broker-alias", "ns-isolation-policy")).isZero());
        assertThat(document.lines().filter(line -> line.equals("# brokers")).count()).isEqualTo(1);
        assertThat(document.lines().filter(line -> line.equals("# ns-isolation-policy")).count()).isEqualTo(1);
        assertThat(document).doesNotContain("# broker-alias\n");
    }

    @Test
    public void testAllModules() {
        String document = generate(root -> assertThat(root.execute("documents", "generate")).isZero());
        assertThat(document.lines().filter(line -> line.equals("# brokers")).count()).isEqualTo(1);
        assertThat(document.lines().filter(line -> line.equals("# ns-isolation-policy")).count()).isEqualTo(1);
        assertThat(document).doesNotContain("# broker-alias\n");
    }

    @Test
    public void testRepeatedInvocation() {
        String document = generate(root -> {
            assertThat(root.execute("documents", "generate", "brokers")).isZero();
            assertThat(root.execute("documents", "generate", "brokers")).isZero();
        });
        assertThat(document.lines().filter(line -> line.equals("# brokers")).count()).isEqualTo(2);
    }
}
