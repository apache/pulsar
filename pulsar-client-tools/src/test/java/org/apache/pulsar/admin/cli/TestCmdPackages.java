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
package org.apache.pulsar.admin.cli;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for packages commands.
 */
public class TestCmdPackages {

    private PulsarAdmin pulsarAdmin;
    private Packages packages;
    private CmdPackages cmdPackages;

    @BeforeMethod
    public void setup() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        packages = mock(Packages.class);
        when(pulsarAdmin.packages()).thenReturn(packages);

        cmdPackages = spy(new CmdPackages(() -> pulsarAdmin));
    }

    @DataProvider(name = "commandsWithoutArgs")
    public static Object[][] commandsWithoutArgs() {
        return new Object[][]{
            {"get-metadata"},
            {"update-metadata"},
            {"upload"},
            {"download"},
            {"list"},
            {"list-versions"},
            {"delete"},
        };
    }

    @Test(timeOut = 60000, dataProvider = "commandsWithoutArgs")
    public void testCommandsWithoutArgs(String command) {
        String packageName = "test-package-name";
        boolean result = cmdPackages.run(new String[]{command});
        assertFalse(result);
    }

    // test command `bin/pulsar-admin packages get-metadata package-name`
    @Test(timeOut = 1000)
    public void testGetMetadataCmd() throws PulsarAdminException {
        String packageName = randomName(8);
        boolean result = cmdPackages.run(new String[]{"get-metadata", packageName});
        assertTrue(result);
        verify(packages, times(1)).getMetadata(eq(packageName));
    }

    // test command `bin/pulsar-admin packages update-metadata package-name --description tests`
    @Test(timeOut = 1000)
    public void testUpdateMetadataCmdWithRequiredArgs() throws PulsarAdminException {
        String packageName = randomName(8);
        boolean result = cmdPackages.run(new String[]{"update-metadata", packageName, "--description", "tests"});
        assertTrue(result);
        verify(packages, times(1))
            .updateMetadata(eq(packageName),
                eq(PackageMetadata.builder().description("tests").properties(Collections.emptyMap()).build()));
    }

    // test command `bin/pulsar-admin packages update-metadata package-name --description tests
    // --contact test@apache.org -PpropertyA=A`
    @Test(timeOut = 1000)
    public void testUpdateMetadataCmdWithAllArgs() throws PulsarAdminException {
        String packageName = randomName(8);
        Map<String, String> properties = new HashMap<>();
        properties.put("propertyA", "A");
        boolean result = cmdPackages.run(new String[]{
            "update-metadata", packageName, "--description", "tests", "--contact", "test@apache.org", "-PpropertyA=A"});
        assertTrue(result);
        verify(packages, times(1))
            .updateMetadata(eq(packageName), eq(PackageMetadata.builder().description("tests")
                .contact("test@apache.org").properties(properties).build()));
    }

    // test command `bin/pulsar-admin packages upload package-name --description tests --path /path/to/package`
    @Test(timeOut = 1000)
    public void testUploadCmdWithRequiredArgs() throws PulsarAdminException {
        String packageName = randomName(8);
        boolean result = cmdPackages.run(new String[]{
            "upload", packageName, "--description", "tests", "--path", "/path/to/package"});
        assertTrue(result);
        verify(packages, times(1)).upload(
            eq(PackageMetadata.builder().description("tests").properties(Collections.emptyMap()).build()),
            eq(packageName),
            eq("/path/to/package"));
    }

    // test command `bin/pulsar-admin packages upload package-name --description tests --contact test@apache.org
    // -PpropertyA=A --path /path/to/package`
    @Test(timeOut = 1000)
    public void testUploadCmdWithAllArgs() throws PulsarAdminException {
        String packageName = randomName(8);
        Map<String, String> properties = new HashMap<>();
        properties.put("propertyA", "A");
        boolean result = cmdPackages.run(new String[]{
            "upload", packageName, "--description", "tests", "--contact", "test@apache.org", "-PpropertyA=A",
            "--path", "/path/to/package"});
        assertTrue(result);
        verify(packages, times(1)).upload(
            eq(PackageMetadata.builder().description("tests").contact("test@apache.org").properties(properties).build()),
            eq(packageName),
            eq("/path/to/package"));
    }

    // test command `bin/pulsar-admin download package-name --path /path/to/package`
    @Test(timeOut = 1000)
    public void testDownloadCmd() throws PulsarAdminException {
        String packageName = randomName(8);
        boolean result = cmdPackages.run(new String[]{"download", packageName, "--path", "/path/to/package"});
        assertTrue(result);
        verify(packages, times(1)).download(eq(packageName), eq("/path/to/package"));
    }

    // test command `bin/pulsar-admin list public/default --type function`
    @Test(timeOut = 1000)
    public void testListCmd() throws PulsarAdminException {
        String namespace = String.format("%s/%s", randomName(4), randomName(4));
        boolean result = cmdPackages.run(new String[]{"list", namespace, "--type", "function"});
        assertTrue(result);
        verify(packages, times(1)).listPackages(eq("function"), eq(namespace));
    }

    // test command `bin/pulsar-admin list-versions package-name`
    @Test(timeOut = 1000)
    public void testListVersionsCmd() throws PulsarAdminException {
        String packageName = randomName(8);
        boolean result = cmdPackages.run(new String[]{"list-versions", packageName});
        assertTrue(result);
        verify(packages, times(1)).listPackageVersions(eq(packageName));
    }

    // test command `bin/pulsar-admin delete package-name`
    @Test(timeOut = 1000)
    public void testDeleteCmd() throws PulsarAdminException {
        String packageName = randomName(8);
        boolean result = cmdPackages.run(new String[]{"delete", packageName});
        assertTrue(result);
        verify(packages, times(1)).delete(eq(packageName));
    }

    private static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
