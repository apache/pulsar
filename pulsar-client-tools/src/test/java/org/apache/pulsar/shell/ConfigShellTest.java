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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.pulsar.shell.config.ConfigStore;
import org.apache.pulsar.shell.config.FileConfigStore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class ConfigShellTest {

    private PulsarShell pulsarShell;
    private ConfigShell configShell;
    private String output;
    private StringWriter stringWriter;

    @BeforeMethod(alwaysRun = true)
    public void before() throws Exception {

        pulsarShell = mock(PulsarShell.class);
        doNothing().when(pulsarShell).reload(any());
        final Path tempJson = Files.createTempFile("pulsar-shell", ".json");

        when(pulsarShell.getConfigStore()).thenReturn(
                new FileConfigStore(tempJson.toFile(),
                        new ConfigStore.ConfigEntry(ConfigStore.DEFAULT_CONFIG, "#comment\ndefault-config=true")));
        configShell = new ConfigShell(pulsarShell, ConfigStore.DEFAULT_CONFIG);
        setConsole();
    }

    private void setConsole() {
        CommandLine commander = configShell.getCommander();
        stringWriter = new StringWriter();
        commander.setOut(new PrintWriter(stringWriter));
    }

    private void cleanOutput() {
        setConsole();
        output = "";
    }

    @Test
    public void testDefault() throws Exception {
        assertTrue(runCommand(new String[]{"list"}));
        assertEquals(output, "default (*)\n");
        cleanOutput();
        assertTrue(runCommand(new String[]{"view", "default"}));
        assertEquals(output, "default-config=true\n\n");
        cleanOutput();

        final Path newClientConf = Files.createTempFile("client", ".conf");
        assertFalse(runCommand(new String[]{"create", "default",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertEquals(output, "Config 'default' already exists.\n");
        cleanOutput();

        assertFalse(runCommand(new String[]{"update", "default",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertEquals(output, "'default' can't be updated.\n");
        cleanOutput();

        assertFalse(runCommand(new String[]{"delete", "default"}));
        assertEquals(output, "'default' can't be deleted.\n");
    }

    @Test
    public void test() throws Exception {
        final Path newClientConf = Files.createTempFile("client", ".conf");

        final byte[] content = ("webServiceUrl=http://localhost:8081/\n" +
                "brokerServiceUrl=pulsar://localhost:6651/\n").getBytes(StandardCharsets.UTF_8);
        Files.write(newClientConf, content);
        assertTrue(runCommand(new String[]{"create", "myclient",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertTrue(output.isEmpty());
        cleanOutput();

        assertNull(pulsarShell.getConfigStore().getLastUsed());

        assertTrue(runCommand(new String[]{"use", "myclient"}));
        assertTrue(output.isEmpty());
        cleanOutput();
        assertEquals(pulsarShell.getConfigStore().getLastUsed(), pulsarShell.getConfigStore()
                .getConfig("myclient"));

        verify(pulsarShell).reload(any());

        assertTrue(runCommand(new String[]{"list"}));
        assertEquals(output, "default\nmyclient (*)\n");
        cleanOutput();

        assertFalse(runCommand(new String[]{"delete", "myclient"}));
        assertEquals(output, "'myclient' is currently used and it can't be deleted.\n");
        cleanOutput();

        assertTrue(runCommand(new String[]{"update", "myclient",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertTrue(output.isEmpty());
        verify(pulsarShell, times(2)).reload(any());

        assertTrue(runCommand(new String[]{"clone", "myclient",
                "--name", "myclient-copied"}));
        assertTrue(output.isEmpty());
        verify(pulsarShell, times(2)).reload(any());

        assertTrue(runCommand(new String[]{"view", "myclient-copied"}));
        assertEquals(output, "webServiceUrl=http://localhost:8081/\nbrokerServiceUrl" +
                "=pulsar://localhost:6651/\n\n");
        cleanOutput();
    }

    @Test
    public void testSetGetProperty() throws Exception {
        final Path newClientConf = Files.createTempFile("client", ".conf");

        final byte[] content = ("webServiceUrl=http://localhost:8081/\n" +
                "brokerServiceUrl=pulsar://localhost:6651/\n").getBytes(StandardCharsets.UTF_8);
        Files.write(newClientConf, content);
        assertTrue(runCommand(new String[]{"create", "myclient",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertTrue(output.isEmpty());
        cleanOutput();

        assertTrue(runCommand(new String[]{"use", "myclient"}));
        assertTrue(output.isEmpty());
        cleanOutput();

        assertTrue(runCommand(new String[]{"get-property", "-p", "webServiceUrl", "myclient"}));
        assertEquals(output, "http://localhost:8081/\n");
        cleanOutput();

        assertTrue(runCommand(new String[]{"set-property", "-p", "newConf",
                "-v", "myValue", "myclient"}));
        verify(pulsarShell, times(2)).reload(any());
        cleanOutput();

        assertTrue(runCommand(new String[]{"get-property", "-p", "newConf", "myclient"}));
        assertEquals(output, "myValue\n");
        cleanOutput();

        assertTrue(runCommand(new String[]{"view", "myclient"}));
        assertEquals(output, "webServiceUrl=http://localhost:8081/\nbrokerServiceUrl" +
                "=pulsar://localhost:6651/\nnewConf=myValue\n\n");
        cleanOutput();

        assertTrue(runCommand(new String[]{"set-property", "-p", "newConf",
                "-v", "myValue2", "myclient"}));
        verify(pulsarShell, times(3)).reload(any());
        cleanOutput();

        assertTrue(runCommand(new String[]{"get-property", "-p", "newConf", "myclient"}));
        assertEquals(output, "myValue2\n");
        cleanOutput();


        assertTrue(runCommand(new String[]{"view", "myclient"}));
        assertEquals(output, "webServiceUrl=http://localhost:8081/\nbrokerServiceUrl" +
                "=pulsar://localhost:6651/\nnewConf=myValue2\n\n");
        cleanOutput();

        assertTrue(runCommand(new String[]{"set-property", "-p", "newConf",
                "-v", "", "myclient"}));
        verify(pulsarShell, times(4)).reload(any());
        cleanOutput();
        assertTrue(runCommand(new String[]{"view", "myclient"}));
        assertEquals(output, "webServiceUrl=http://localhost:8081/\nbrokerServiceUrl" +
                "=pulsar://localhost:6651/\nnewConf=\n\n");
        cleanOutput();

        assertTrue(runCommand(new String[]{"get-property", "-p", "newConf", "myclient"}));
        assertTrue(output.isEmpty());
        cleanOutput();

    }

    private boolean runCommand(String[] x) throws Exception {
        try {
            CommandLine commander = configShell.getCommander();
            return commander.execute(x) == 0;
        } finally {
            output = stringWriter.toString();
            setConsole();
        }
    }

    @Test
    public void testResolveLocalFile() throws Exception {
        assertEquals(ConfigShell.resolveLocalFile("myfile").getAbsolutePath(),
                new File("myfile").getAbsolutePath());
        assertEquals(ConfigShell.resolveLocalFile("mydir/myfile.txt").getAbsolutePath(),
                new File("mydir/myfile.txt").getAbsolutePath());
        assertEquals(ConfigShell.resolveLocalFile("myfile", "current").getAbsolutePath(),
                new File("current/myfile").getAbsolutePath());
        assertEquals(ConfigShell.resolveLocalFile("mydir/myfile.txt", "current").getAbsolutePath(),
                new File("current/mydir/myfile.txt").getAbsolutePath());

        assertEquals(ConfigShell.resolveLocalFile("/tmp/absolute.txt").getAbsolutePath(),
                new File("/tmp/absolute.txt").getAbsolutePath());

        assertEquals(ConfigShell.resolveLocalFile("/tmp/absolute.txt", "current").getAbsolutePath(),
                new File("/tmp/absolute.txt").getAbsolutePath());
    }
}