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
package org.apache.pulsar.shell;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.pulsar.admin.cli.PulsarAdminSupplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.cli.CmdProduce;
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarShellTest {

    private static final Logger log = LoggerFactory.getLogger(PulsarShellTest.class);

    private PulsarAdmin pulsarAdmin;

    private Topics topics;

    static class MockLineReader extends LineReaderImpl implements PulsarShell.InteractiveLineReader {

        private BlockingQueue<String> commandsQueue = new LinkedBlockingQueue<>();

        public MockLineReader(Terminal terminal) throws IOException {
            super(terminal);
        }

        public void addCmd(String cmd) {
            commandsQueue.add(cmd);
        }

        @Override
        @SneakyThrows
        public String readLine() throws UserInterruptException, EndOfFileException {
            final String cmd = commandsQueue.take();
            log.info("writing command: {}", cmd);
            return cmd;

        }

        @Override
        public List<String> parseLine(String line) {
            return getParser().parse(line, 0).words();
        }
    }

    private static class TestPulsarShell extends PulsarShell {

        private final PulsarAdmin pulsarAdmin;
        AtomicReference<CmdProduce> cmdProduceHolder = new AtomicReference<>();
        Integer exitCode;

        public TestPulsarShell(String[] args, Properties props, PulsarAdmin pulsarAdmin) throws IOException {
            super(args, props);
            this.pulsarAdmin = pulsarAdmin;
        }

        @Override
        protected AdminShell createAdminShell(Properties properties) throws Exception {
            final AdminShell adminShell = new AdminShell(properties);
            final PulsarAdminSupplier supplier = mock(PulsarAdminSupplier.class);
            when(supplier.get()).thenReturn(pulsarAdmin);
            adminShell.setPulsarAdminSupplier(supplier);
            return adminShell;
        }

        @Override
        protected ClientShell createClientShell(Properties properties) {
            final CmdProduce cmdProduce = mock(CmdProduce.class);
            cmdProduceHolder.set(cmdProduce);
            return new ClientShell(properties) {{
                this.produceCommand = cmdProduce;
            }};
        }

        @Override
        protected void exit(int exitCode) {
            this.exitCode = exitCode;
            if (exitCode != 0) {
                throw new SystemExitCalledException(exitCode);
            }
        }
    }

    private static class SystemExitCalledException extends RuntimeException {
        private final int code;

        public SystemExitCalledException(int code) {
            this.code = code;
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        when(pulsarAdmin.topics()).thenReturn(topics);
    }


    @Test
    public void testInteractiveMode() throws Exception {
        Terminal terminal = TerminalBuilder.builder().build();
        final MockLineReader linereader = new MockLineReader(terminal);

        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");
        linereader.addCmd("admin topics create my-topic --metadata a=b ");
        linereader.addCmd("client produce -m msg my-topic");
        linereader.addCmd("quit");
        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{}, props, pulsarAdmin);
        testPulsarShell.run((a) -> linereader, (a) -> terminal);
        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(testPulsarShell.cmdProduceHolder.get()).run();
        assertEquals((int) testPulsarShell.exitCode, 0);

    }

    @Test
    public void testFileMode() throws Exception {
        Terminal terminal = TerminalBuilder.builder().build();
        final MockLineReader linereader = new MockLineReader(terminal);
        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");

        final String shellFile = Thread.currentThread()
                .getContextClassLoader().getResource("test-shell-file").getFile();

        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{"-f", shellFile},
                props, pulsarAdmin);
        testPulsarShell.run((a) -> linereader, (a) -> terminal);
        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(testPulsarShell.cmdProduceHolder.get()).run();
    }

    @Test
    public void testFileModeExitOnError() throws Exception {
        Terminal terminal = TerminalBuilder.builder().build();
        final MockLineReader linereader = new MockLineReader(terminal);
        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");

        final String shellFile = Thread.currentThread()
                .getContextClassLoader().getResource("test-shell-file-error").getFile();

        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{"-f", shellFile, "--fail-on-error"},
                props, pulsarAdmin);
        try {
            testPulsarShell.run((a) -> linereader, (a) -> terminal);
            fail();
        } catch (SystemExitCalledException ex) {
            assertEquals(ex.code, 1);
        }

        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(testPulsarShell.cmdProduceHolder.get(), times(0)).run();
    }

    @Test
    public void testSubstituteVariables() throws Exception {
        Map<String, String> vars = new HashMap<>();
        vars.put("mytopic", "the-topic");
        assertEquals(
                PulsarShell.substituteVariables(Arrays.asList("admin", "topics", "create", "${mytopic}"), vars),
                Arrays.asList("admin", "topics", "create", "the-topic")
        );
        assertEquals(
                PulsarShell.substituteVariables(Arrays.asList("admin", "topics", "create", "\\${mytopic}"), vars),
                Arrays.asList("admin", "topics", "create", "${mytopic}")
        );
        assertEquals(
                PulsarShell.substituteVariables(Arrays.asList("admin", "topics", "create", "${MYTOPIC}"), vars),
                Arrays.asList("admin", "topics", "create", "${MYTOPIC}")
        );
        assertEquals(
                PulsarShell.substituteVariables(Arrays.asList("admin", "topics", "create", "$mytopic"), vars),
                Arrays.asList("admin", "topics", "create", "the-topic")
        );
        assertEquals(
                PulsarShell.substituteVariables(Arrays.asList("admin", "topics", "create", "\\$mytopic"), vars),
                Arrays.asList("admin", "topics", "create", "$mytopic")
        );

    }
}