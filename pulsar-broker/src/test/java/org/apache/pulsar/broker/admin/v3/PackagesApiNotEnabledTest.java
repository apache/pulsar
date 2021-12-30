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
package org.apache.pulsar.broker.admin.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PackagesApiNotEnabledTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // not enable Package Management Service
        conf.setEnablePackagesManagement(false);
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60000)
    public void testPackagesOperationsWithoutPackagesServiceEnabled() {
        // download package api should return 503 Service Unavailable exception
        String unknownPackageName = "function://public/default/unknown@v1";
        try {
            admin.packages().download(unknownPackageName, "/test/unknown");
            fail("should throw 503 error");
        } catch (PulsarAdminException e) {
            assertEquals(503, e.getStatusCode());
        }

        // get metadata api should return 503 Service Unavailable exception
        try {
            admin.packages().getMetadata(unknownPackageName);
            fail("should throw 503 error");
        } catch (PulsarAdminException e) {
            assertEquals(503, e.getStatusCode());
        }

        // update metadata api should return 503 Service Unavailable exception
        try {
            admin.packages().updateMetadata(unknownPackageName,
                    PackageMetadata.builder().description("unknown").build());
            fail("should throw 503 error");
        } catch (PulsarAdminException e) {
            assertEquals(503, e.getStatusCode());
        }

        // list all the packages api should return 503 Service Unavailable exception
        try {
            admin.packages().listPackages("function", "unknown/unknown");
            fail("should throw 503 error");
        } catch (PulsarAdminException e) {
            assertEquals(503, e.getStatusCode());
        }

        // list all the versions api should return 503 Service Unavailable exception
        try {
            admin.packages().listPackageVersions(unknownPackageName);
            fail("should throw 503 error");
        } catch (PulsarAdminException e) {
            assertEquals(503, e.getStatusCode());
        }
    }
}
