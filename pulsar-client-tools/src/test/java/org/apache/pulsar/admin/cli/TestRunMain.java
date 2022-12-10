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

import static org.testng.Assert.assertEquals;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.testng.annotations.Test;

public class TestRunMain {

    @Test
    public void runMainNoArguments() throws Exception {
        PulsarAdminTool.resetLastExitCode();
        PulsarAdminTool.setAllowSystemExit(false);
        PulsarAdminTool.main(new String[0]);
        assertEquals(PulsarAdminTool.getLastExitCode(), 0);
    }

    @Test
    public void runMainDummyConfigFile() throws Exception {
        PulsarAdminTool.resetLastExitCode();
        PulsarAdminTool.setAllowSystemExit(false);
        Path dummyEmptyFile = Files.createTempFile("test", ".conf");
        PulsarAdminTool.main(new String[]{dummyEmptyFile.toAbsolutePath().toString()});
        assertEquals(PulsarAdminTool.getLastExitCode(), 1);
    }

    @Test
    public void testRunWithTlsProviderFlag() throws Exception {
        var pulsarAdminTool = new PulsarAdminTool(new Properties());
        pulsarAdminTool.run(new String[]{
                "--admin-url", "https://localhost:8081",
                "--tls-provider", "JDK",
                "tenants"});
        assertEquals(pulsarAdminTool.rootParams.tlsProvider, "JDK");
    }

    @Test
    public void testRunWithTlsProviderConfigFile() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("webserviceTlsProvider", "JDK");
        var pulsarAdminTool = new PulsarAdminTool(properties);
        pulsarAdminTool.run(new String[]{
                "--admin-url", "https://localhost:8081",
                "tenants"});
        assertEquals(pulsarAdminTool.rootParams.tlsProvider, "JDK");
    }

    @Test
    public void testRunWithTlsProviderFlagWithConfigFile() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("webserviceTlsProvider", "JDK");
        var pulsarAdminTool = new PulsarAdminTool(properties);
        pulsarAdminTool.run(new String[]{
                "--admin-url", "https://localhost:8081",
                "--tls-provider", "OPENSSL",
                "tenants"});
        assertEquals(pulsarAdminTool.rootParams.tlsProvider, "OPENSSL");
    }
}
