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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.CustomLog;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.cli.CmdProduce;
import org.jline.reader.EndOfFileException;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CustomLog
public class PulsarShellTest {

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
            log.info().attr("command", cmd).log("Writing command");
            return cmd;

        }

        @Override
        public List<String> parseLine(String line) {
            return getParser().parse(line, 0, Parser.ParseContext.SPLIT_LINE).words();
        }
    }

    private static class TestPulsarShell extends PulsarShell {

        private final PulsarAdmin pulsarAdmin;
        AtomicReference<CmdProduce> cmdProduceHolder = new AtomicReference<>();
        Consumer<CmdProduce> cmdProduceCustomizer = cmdProduce -> { };
        Integer exitCode;

        public TestPulsarShell(String[] args, Properties props, PulsarAdmin pulsarAdmin) throws IOException {
            super(args, props);
            this.pulsarAdmin = pulsarAdmin;
        }

        @Override
        protected AdminShell createAdminShell(Properties properties) throws Exception {
            final AdminShell adminShell = new AdminShell(properties);
            PulsarAdminBuilder builder = mock(PulsarAdminBuilder.class);
            doReturn(pulsarAdmin).when(builder).build();
            adminShell.getPulsarAdminSupplier().setAdminBuilder(builder);
            return adminShell;
        }

        @Override
        protected ClientShell createClientShell(Properties properties) {
            final CmdProduce cmdProduce = mock(CmdProduce.class);
            cmdProduceCustomizer.accept(cmdProduce);
            cmdProduceHolder.set(cmdProduce);
            ClientShell clientShell = new ClientShell(properties);
            clientShell.replaceProducerCommand(cmdProduce);
            return clientShell;
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
    @SuppressWarnings("unchecked")
    public void testInteractiveMode() throws Exception {
        Terminal terminal = TerminalBuilder.builder().build();
        final MockLineReader linereader = new MockLineReader(terminal);

        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");
        linereader.addCmd("admin topics create my-topic --metadata a=b ");
        linereader.addCmd("client produce -m msg my-topic");
        linereader.addCmd("client produce -m \"hello pulsar\" my-topic");
        linereader.addCmd("quit");
        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{}, props, pulsarAdmin);
        testPulsarShell.run((a) -> linereader, () -> terminal);
        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(testPulsarShell.cmdProduceHolder.get(), times(2)).call();
        assertEquals((int) testPulsarShell.exitCode, 0);

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFileMode() throws Exception {
        Terminal terminal = TerminalBuilder.builder().build();
        final MockLineReader linereader = new MockLineReader(terminal);
        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");

        final String shellFile = Thread.currentThread()
                .getContextClassLoader().getResource("test-shell-file").getFile();

        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{"-f", shellFile},
                props, pulsarAdmin);
        testPulsarShell.run((a) -> linereader, () -> terminal);
        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(testPulsarShell.cmdProduceHolder.get()).call();
    }

    @Test
    @SuppressWarnings("unchecked")
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
            testPulsarShell.run((a) -> linereader, () -> terminal);
            fail();
        } catch (SystemExitCalledException ex) {
            assertEquals(ex.code, 1);
        }

        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(testPulsarShell.cmdProduceHolder.get(), times(0)).run();
    }

    @Test
    public void testInterruptRunningCommand() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Terminal terminal = TerminalBuilder.builder()
                .streams(new ByteArrayInputStream(new byte[0]), output)
                .dumb(true)
                .build();
        final MockLineReader linereader = new MockLineReader(terminal);
        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");
        linereader.addCmd("client produce -m msg my-topic");

        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{}, props, pulsarAdmin);
        final CountDownLatch commandStarted = new CountDownLatch(1);
        final AtomicBoolean commandInterrupted = new AtomicBoolean();
        testPulsarShell.cmdProduceCustomizer = cmdProduce -> {
            try {
                doAnswer(invocation -> {
                    commandStarted.countDown();
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                    } catch (InterruptedException e) {
                        commandInterrupted.set(true);
                        throw e;
                    }
                    return 0;
                }).when(cmdProduce).call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // Press Ctrl-C while the command is running, then quit
        final Thread signaller = new Thread(() -> {
            try {
                assertTrue(commandStarted.await(10, TimeUnit.SECONDS));
                testPulsarShell.handleSignal(Terminal.Signal.INT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                linereader.addCmd("quit");
            }
        });
        signaller.start();
        testPulsarShell.run((a) -> linereader, () -> terminal);
        signaller.join();

        assertTrue(commandInterrupted.get());
        assertEquals((int) testPulsarShell.exitCode, 0);
        final String out = output.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Interrupted"), out);
        assertFalse(out.contains("InterruptShellException"), out);
    }

    @Test
    public void testInterruptRacingCommandCompletionDoesNotReachNextCommand() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Terminal terminal = TerminalBuilder.builder()
                .streams(new ByteArrayInputStream(new byte[0]), output)
                .dumb(true)
                .build();
        final MockLineReader linereader = new MockLineReader(terminal);
        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");
        linereader.addCmd("client produce -m first my-topic");
        linereader.addCmd("client produce -m second my-topic");
        linereader.addCmd("quit");

        final TestPulsarShell testPulsarShell = new TestPulsarShell(new String[]{}, props, pulsarAdmin);
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch firstMayFinish = new CountDownLatch(1);
        final CountDownLatch firstFinished = new CountDownLatch(1);
        final AtomicBoolean secondInterrupted = new AtomicBoolean();
        final AtomicBoolean secondRan = new AtomicBoolean();
        testPulsarShell.cmdProduceCustomizer = cmdProduce -> {
            try {
                doAnswer(invocation -> {
                    firstStarted.countDown();
                    assertTrue(firstMayFinish.await(10, TimeUnit.SECONDS));
                    firstFinished.countDown();
                    return 0;
                }).doAnswer(invocation -> {
                    secondRan.set(true);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        secondInterrupted.set(true);
                    }
                    return 0;
                }).when(cmdProduce).call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // Deliver Ctrl-C after the first command's work is done but before the shell retires it: holding the
        // lock keeps the shell from retiring the command until the signal has been delivered.
        final Thread signaller = new Thread(() -> {
            try {
                assertTrue(firstStarted.await(10, TimeUnit.SECONDS));
                synchronized (testPulsarShell.interruptLock) {
                    firstMayFinish.countDown();
                    assertTrue(firstFinished.await(10, TimeUnit.SECONDS));
                    testPulsarShell.handleSignal(Terminal.Signal.INT);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        signaller.start();
        testPulsarShell.run((a) -> linereader, () -> terminal);
        signaller.join();

        assertTrue(secondRan.get());
        assertFalse(secondInterrupted.get(), "the interrupt for the first command reached the second one");
        assertEquals((int) testPulsarShell.exitCode, 0);
        final String out = output.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Interrupted"), out);
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