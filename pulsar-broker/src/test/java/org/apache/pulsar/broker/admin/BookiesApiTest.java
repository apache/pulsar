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

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class BookiesApiTest extends MockedPulsarServiceBaseTest {

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
    public void testBasic() throws Exception {
        // no map
        BookiesRackConfiguration conf = (BookiesRackConfiguration) admin.bookies().getBookiesRackInfo();
        assertTrue(conf.isEmpty());

        String bookie0 = "127.0.0.1:3181";
        String bookie1 = "127.0.0.2:3181";

        // get bookie doesn't exist
        try {
            admin.bookies().getBookieRackInfo(bookie0);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }

        // update the bookie info
        BookieInfo newInfo0 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.1")
                .build();
        BookieInfo newInfo1 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.2")
                .build();
        admin.bookies().updateBookieRackInfo(bookie0, "default", newInfo0);
        BookieInfo readInfo0 = admin.bookies().getBookieRackInfo(bookie0);
        assertEquals(newInfo0, readInfo0);
        conf = (BookiesRackConfiguration) admin.bookies().getBookiesRackInfo();
        // number of groups
        assertEquals(1, conf.size());
        assertEquals(Optional.of(newInfo0), conf.getBookie(bookie0));

        admin.bookies().updateBookieRackInfo(bookie1, "default", newInfo1);
        BookieInfo readInfo1 = admin.bookies().getBookieRackInfo(bookie1);
        assertEquals(newInfo1, readInfo1);
        conf = (BookiesRackConfiguration) admin.bookies().getBookiesRackInfo();
        // number of groups
        assertEquals(1, conf.size());
        assertEquals(Optional.of(newInfo0), conf.getBookie(bookie0));
        assertEquals(Optional.of(newInfo1), conf.getBookie(bookie1));

        admin.bookies().deleteBookieRackInfo(bookie0);
        try {
            admin.bookies().getBookieRackInfo(bookie0);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        assertEquals(newInfo1, admin.bookies().getBookieRackInfo(bookie1));

        admin.bookies().deleteBookieRackInfo(bookie1);
        try {
            admin.bookies().getBookieRackInfo(bookie1);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }

        conf = (BookiesRackConfiguration) admin.bookies().getBookiesRackInfo();
        assertTrue(conf.isEmpty());

        BookiesClusterInfo bookies = admin.bookies().getBookies();
        log.info("bookies info {}", bookies);
        assertEquals(bookies.getBookies().size(),
                pulsar.getBookKeeperClient()
                .getMetadataClientDriver()
                .getRegistrationClient()
                .getAllBookies()
                .get()
                .getValue()
                .size());

        // test invalid rack name
        // use rack aware placement policy
        String errorMsg = "Bookie 'rack' parameter is invalid, When `RackawareEnsemblePlacementPolicy` is enabled, "
            + "the rack name is not allowed to contain slash (`/`) except for the beginning and end of the rack name "
            + "string. When `RegionawareEnsemblePlacementPolicy` is enabled, the rack name can only contain "
            + "one slash (`/`) except for the beginning and end of the rack name string.";

        BookieInfo newInfo3 = BookieInfo.builder()
            .rack("/rack/a")
            .hostname("127.0.0.2")
            .build();
        try {
            admin.bookies().updateBookieRackInfo(bookie0, "default", newInfo3);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(412, e.getStatusCode());
            assertEquals(errorMsg, e.getMessage());
        }

        BookieInfo newInfo4 = BookieInfo.builder()
            .rack("/rack")
            .hostname("127.0.0.2")
            .build();
        try {
            admin.bookies().updateBookieRackInfo(bookie0, "default", newInfo4);
        } catch (PulsarAdminException e) {
            fail();
        }

        // enable region aware placement policy
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setBookkeeperClientRegionawarePolicyEnabled(true);
        doReturn(configuration).when(pulsar).getConfiguration();
        BookieInfo newInfo5 = BookieInfo.builder()
            .rack("/region/rack/a")
            .hostname("127.0.0.2")
            .build();
        try {
            admin.bookies().updateBookieRackInfo(bookie0, "default", newInfo5);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(412, e.getStatusCode());
            assertEquals(errorMsg, e.getMessage());
        }

        BookieInfo newInfo6 = BookieInfo.builder()
            .rack("/region/rack/")
            .hostname("127.0.0.2")
            .build();
        try {
            admin.bookies().updateBookieRackInfo(bookie0, "default", newInfo6);
        } catch (PulsarAdminException e) {
            fail();
        }
    }

}
