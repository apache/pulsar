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
import static org.testng.Assert.assertNotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
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

    @Test
    public void testMainArgs() throws Exception {
        String tlsTrustCertsFilePathInFile = "ca-file.cert";
        String tlsTrustCertsFilePathInArg = "ca-arg.cert";
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("tlsTrustCertsFilePath=" + tlsTrustCertsFilePathInFile);
        printWriter.println("tlsAllowInsecureConnection=" + false);
        printWriter.println("tlsEnableHostnameVerification=" + false);

        printWriter.close();
        testConfigFile.deleteOnExit();

        String argStrTemp = "%s %s --admin-url https://url:4443 " + "topics stats persistent://prop/cluster/ns/t1";
        boolean prevValue = PulsarAdminTool.allowSystemExit;
        PulsarAdminTool.allowSystemExit = false;

        String argStr = argStr = argStrTemp.format(argStrTemp, testConfigFile.getAbsolutePath(),
                "--tls-trust-cert-path " + tlsTrustCertsFilePathInArg);
        PulsarAdminTool tool = PulsarAdminTool.execute(argStr.split(" "));
        assertNotNull(tool);
        PulsarAdminBuilderImpl builder = (PulsarAdminBuilderImpl) tool.pulsarAdminSupplier.adminBuilder;
        assertEquals(builder.getConf().getTlsTrustCertsFilePath(), tlsTrustCertsFilePathInArg);
        PulsarAdminTool.allowSystemExit = prevValue;
    }
}
