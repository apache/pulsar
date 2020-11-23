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
package org.apache.pulsar.packages.management.core.common;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PackageNameTest {

    @DataProvider(name = "packageNames")
    public static Object[][] packageNamesProvider() {
        return new Object[][]{
            {"function", "f-tenant", "f-ns", "f-name", "f-version"},
            {"sink", "s-tenant", "s-ns", "s-name", "s-version"},
            {"source", "s-tenant", "s-ns", "s-name", "s-version"},
        };
    }

    @Test(dataProvider = "packageNames")
    public void testPartName(String type, String tenant, String ns, String name, String version) {
        PackageName packageName = PackageName.get(type, tenant, ns, name, version);
        Assert.assertEquals(packageName.getPkgType().toString(), type);
        Assert.assertEquals(packageName.getTenant(), tenant);
        Assert.assertEquals(packageName.getNamespace(), ns);
        Assert.assertEquals(packageName.getName(), name);
        Assert.assertEquals(packageName.getVersion(), version);
        Assert.assertEquals(packageName.toString(), type + "://" + tenant + "/" + ns + "/" + name + "@" + version);

        String longName = tenant + "/" + ns + "/" + name;
        PackageName packageName1 = PackageName.get(type, longName, version);
        Assert.assertEquals(packageName1.getPkgType().toString(), type);
        Assert.assertEquals(packageName.getTenant(), tenant);
        Assert.assertEquals(packageName.getNamespace(), ns);
        Assert.assertEquals(packageName1.getName(), name);
        Assert.assertEquals(packageName1.getVersion(), version);
        Assert.assertEquals(packageName1.toString(), type + "://" + tenant + "/" + ns + "/" + name + "@" + version);

        String fullName = type + "://" + tenant + "/" + ns + "/" + name + "@" + version;
        PackageName packageName2 = PackageName.get(fullName);
        Assert.assertEquals(packageName2.getPkgType().toString(), type);
        Assert.assertEquals(packageName.getTenant(), tenant);
        Assert.assertEquals(packageName.getNamespace(), ns);
        Assert.assertEquals(packageName2.getName(), name);
        Assert.assertEquals(packageName2.getVersion(), version);
        Assert.assertEquals(packageName2.toString(), type + "://" + tenant + "/" + ns + "/" + name + "@" + version);

    }

    @Test
    public void testPackageNameErrors() {

        try {
            PackageName.get("function:///public/default/test-error@v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }


        // invalid package type
        try {
            PackageName.get("functions", "public", "default", "test-error", "v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        try {
            PackageName.get("functions://public/default/test-error@v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // invalid namespace name
        try {
            PackageName.get("function", "public/default", "default", "test-error", "v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // invalid package name
        try {
            PackageName.get("function", "public/default/name@v1", "");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        try {
            PackageName.get("function://public/default/name#v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // invalid package version
        try {
            PackageName.get("function://public/default/test-error-version/v2");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        PackageName name = PackageName.get("function://public/default/test");
        Assert.assertEquals("function://public/default/test@latest", name.toString());
    }
}
