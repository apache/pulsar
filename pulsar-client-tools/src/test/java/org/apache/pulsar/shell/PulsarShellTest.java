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
package org.apache.pulsar.shell;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.cli.CmdProduce;
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



public class PulsarShellTest {

    private static final Logger log = LoggerFactory.getLogger(PulsarShellTest.class);

    private PulsarAdminBuilder pulsarAdminBuilder;
    private PulsarAdmin pulsarAdmin;

    private Topics topics;

    static class MockLineReader extends LineReaderImpl {

        private BlockingQueue<String> commandsQueue = new LinkedBlockingQueue<>();

        public MockLineReader(Terminal terminal) throws IOException {
            super(terminal);
        }

        public void addCmd(String cmd) {
            commandsQueue.add(cmd);
        }

        @Override
        @SneakyThrows
        public String readLine(String prompt) throws UserInterruptException, EndOfFileException {
            final String cmd = commandsQueue.take();
            log.info("writing command: {}", cmd);
            return cmd;

        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        pulsarAdminBuilder = mock(PulsarAdminBuilder.class);
        pulsarAdmin = mock(PulsarAdmin.class);
        when(pulsarAdminBuilder.build()).thenReturn(pulsarAdmin);
        topics = mock(Topics.class);
        when(pulsarAdmin.topics()).thenReturn(topics);
    }


    @Test
    public void mainTest() throws Exception{
        AtomicReference<CmdProduce> cmdProduceHolder = new AtomicReference<>();
        Terminal terminal = TerminalBuilder.builder().build();
        final MockLineReader linereader = new MockLineReader(terminal);

        final Properties props = new Properties();
        props.setProperty("webServiceUrl", "http://localhost:8080");
        linereader.addCmd("admin topics create my-topic --metadata a=b ");
        linereader.addCmd("client produce -m msg my-topic");
        linereader.addCmd("quit");
        new PulsarShell(){
            @Override
            protected AdminShell createAdminShell(Properties properties) throws Exception {
                return new AdminShell(properties) {
                    @Override
                    protected PulsarAdminBuilder createAdminBuilder(Properties properties) {
                        return pulsarAdminBuilder;
                    }
                };
            }

            @Override
            protected ClientShell createClientShell(Properties properties) {
                final ClientShell clientShell = new ClientShell(properties);
                final Object current = Whitebox.getInternalState(clientShell, "produceCommand");
                cmdProduceHolder.set(spy((CmdProduce) current));
                Whitebox.setInternalState(clientShell, "produceCommand", cmdProduceHolder.get());
                return clientShell;
            }

        }.run(props, (a) -> linereader, (a) -> terminal);
        verify(topics).createNonPartitionedTopic(eq("persistent://public/default/my-topic"), any(Map.class));
        verify(cmdProduceHolder.get()).run();

    }

}