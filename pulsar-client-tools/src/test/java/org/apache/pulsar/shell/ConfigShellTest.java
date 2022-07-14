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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.beust.jcommander.internal.Console;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.pulsar.shell.config.ConfigStore;
import org.apache.pulsar.shell.config.FileConfigStore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigShellTest {

    private PulsarShell pulsarShell;
    private ConfigShell configShell;
    private final List<String> output = new ArrayList<>();

    @BeforeMethod(alwaysRun = true)
    public void before() throws Exception {

        pulsarShell = spy(mock(PulsarShell.class));
        doNothing().when(pulsarShell).reload(any());
        final Path tempJson = Files.createTempFile("pulsar-shell", ".json");

        when(pulsarShell.getConfigStore()).thenReturn(
                new FileConfigStore(tempJson.toFile(),
                        new ConfigStore.ConfigEntry(ConfigStore.DEFAULT_CONFIG, "#comment\ndefault-config=true")));
        configShell = new ConfigShell(pulsarShell);
        configShell.setupState(new Properties());

        configShell.getJCommander().setConsole(new Console() {
            @Override
            public void print(String msg) {
                System.out.print("got: " + msg);
                output.add(msg);
            }

            @Override
            public void println(String msg) {
                System.out.println("got: " + msg);
                output.add(msg);
            }

            @Override
            public char[] readPassword(boolean echoInput) {
                return new char[0];
            }
        });

    }

    @Test
    public void testDefault() throws Exception {
        assertTrue(configShell.runCommand(new String[]{"list"}));
        assertEquals(output, Arrays.asList("default (*)"));
        output.clear();
        assertTrue(configShell.runCommand(new String[]{"view", "default"}));
        assertEquals(output.get(0), "default-config=true\n");
        output.clear();

        final Path newClientConf = Files.createTempFile("client", ".conf");
        assertFalse(configShell.runCommand(new String[]{"create", "default",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertEquals(output, Arrays.asList("Config 'default' already exists."));
        output.clear();

        assertFalse(configShell.runCommand(new String[]{"update", "default",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertEquals(output, Arrays.asList("'default' can't be updated."));
        output.clear();

        assertFalse(configShell.runCommand(new String[]{"delete", "default"}));
        assertEquals(output, Arrays.asList("'default' can't be deleted."));
    }

    @Test
    public void test() throws Exception {
        final Path newClientConf = Files.createTempFile("client", ".conf");

        final byte[] content = ("webServiceUrl=http://localhost:8081/\n" +
                "brokerServiceUrl=pulsar://localhost:6651/\n").getBytes(StandardCharsets.UTF_8);
        Files.write(newClientConf, content);
        assertTrue(configShell.runCommand(new String[]{"create", "myclient",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertTrue(output.isEmpty());
        output.clear();

        assertNull(pulsarShell.getConfigStore().getLastUsed());

        assertTrue(configShell.runCommand(new String[]{"use", "myclient"}));
        assertTrue(output.isEmpty());
        output.clear();
        assertEquals(pulsarShell.getConfigStore().getLastUsed(), pulsarShell.getConfigStore()
                .getConfig("myclient"));

        verify(pulsarShell).reload(any());

        assertTrue(configShell.runCommand(new String[]{"list"}));
        assertEquals(output, Arrays.asList("default", "myclient (*)"));
        output.clear();

        assertFalse(configShell.runCommand(new String[]{"delete", "myclient"}));
        assertEquals(output, Arrays.asList("'myclient' is currently used and it can't be deleted."));
        output.clear();

        assertTrue(configShell.runCommand(new String[]{"update", "myclient",
                "--file", newClientConf.toFile().getAbsolutePath()}));
        assertTrue(output.isEmpty());
        verify(pulsarShell, times(2)).reload(any());
    }
}