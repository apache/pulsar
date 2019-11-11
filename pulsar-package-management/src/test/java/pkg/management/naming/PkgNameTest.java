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
 *
 */

package pkg.management.naming;

import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PkgNameTest {

    @DataProvider(name = "pkgNames")
    public static Object[][] namePartsProvider() {
        return new Object[][]{
            {"function", "public", "function-ns", "test-function", "v1"},
            {"sink", "public", "sink-ns", "test-sink", "v2"},
            {"source", "public", "source-ns", "test-source", "v3"},
        };
    }

    @Test(dataProvider = "pkgNames")
    public void testPartName(String type, String tenant, String ns, String name, String version) {
        PkgName pkgName = PkgName.get(type, tenant, ns, name, version);
        Assert.assertEquals(pkgName.getPkgType().toString(), type);
        Assert.assertEquals(pkgName.getNamespaceName(), NamespaceName.get(tenant, ns));
        Assert.assertEquals(pkgName.getName(), name);
        Assert.assertEquals(pkgName.getVersion(), version);
        Assert.assertEquals(pkgName.toString(), type + "://" + tenant + "/" + ns + "/" + name + "@" + version);

        String longName = tenant + "/" + ns + "/" + name;
        PkgName pkgName1 = PkgName.get(type, longName, version);
        Assert.assertEquals(pkgName1.getPkgType().toString(), type);
        Assert.assertEquals(pkgName1.getNamespaceName(), NamespaceName.get(tenant, ns));
        Assert.assertEquals(pkgName1.getName(), name);
        Assert.assertEquals(pkgName1.getVersion(), version);
        Assert.assertEquals(pkgName1.toString(), type + "://" + tenant + "/" + ns + "/" + name + "@" + version);

        String fullName = type + "://" + tenant + "/" + ns + "/" + name + "@" + version;
        PkgName pkgName2 = PkgName.get(fullName);
        Assert.assertEquals(pkgName2.getPkgType().toString(), type);
        Assert.assertEquals(pkgName2.getNamespaceName(), NamespaceName.get(tenant, ns));
        Assert.assertEquals(pkgName2.getName(), name);
        Assert.assertEquals(pkgName2.getVersion(), version);
        Assert.assertEquals(pkgName2.toString(), type + "://" + tenant + "/" + ns + "/" + name + "@" + version);

    }

    @Test
    public void testPkgNameErrors() {

        try {
            PkgName.get("function:///public/default/test-error@v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }


        // invalid package type
        try {
            PkgName.get("functions", "public", "default", "test-error", "v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        try {
            PkgName.get("functions://public/default/test-error@v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // invalid namespace name
        try {
            PkgName.get("function", "public/default", "default", "test-error", "v1");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // invalid package name
        try {
            PkgName.get("function", "public/default/name@v1", "");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // invalid package version
        try {
            PkgName.get("function://public/default/test-error-version/v2");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

    }

}
