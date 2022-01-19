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
package org.apache.pulsar.broker.admin;

import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Map;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiDynamicConfigurationsTest extends MockedPulsarServiceBaseTest {
    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void TestGetAllDynamicConfigurations() throws Exception {
        Map<String,String> configs = admin.brokers().getAllDynamicConfigurations();
        assertNotNull(configs);
    }

    @Test
    public void TestDeleteDynamicConfiguration() throws Exception {
        admin.brokers().deleteDynamicConfiguration("dispatcherMinReadBatchSize");
    }

    @Test
    public void TestDeleteInvalidDynamicConfiguration() {
        try {
            admin.brokers().deleteDynamicConfiguration("errorName");
            fail("exception should be thrown");
        } catch (Exception e) {
            if (e instanceof PulsarAdminException) {
                assertEquals(((PulsarAdminException) e).getStatusCode(), Response.Status.PRECONDITION_FAILED.getStatusCode());
            } else {
                fail("PulsarAdminException should be thrown");
            }
        }
    }
}
