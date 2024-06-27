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
package org.apache.pulsar.broker.admin;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.ExtBookieInfo;
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
            assertEquals(pae.getHttpError(), "Bookie rack placement configuration not found: " + bookie0);
        }

        // delete bookie doesn't exist
        try {
            admin.bookies().deleteBookieRackInfo(bookie0);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
            assertEquals(pae.getHttpError(), "Bookie rack placement configuration not found: " + bookie0);
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

    public void testUpdateBookieRackInfo() throws Exception {
        BookiesRackConfiguration conf = admin.bookies().getBookiesRackInfo();
        assertTrue(conf.isEmpty());

        String group = "default";
        String bookie0 = "127.0.0.1:3181";
        BookieInfo newInfo0 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.1")
                .build();

        // 1. update
        admin.bookies().updateBookieRackInfo(bookie0, group, newInfo0);
        BookieInfo readInfo0 = admin.bookies().getBookieRackInfo(bookie0);
        assertEquals(newInfo0.getRack(), readInfo0.getRack());
        assertEquals(newInfo0.getHostname(), readInfo0.getHostname());


        ExtBookieInfo newInfo1 = ExtBookieInfo.builder()
                .address("127.0.0.2:3181")
                .group("default")
                .rack("/rack2")
                .hostname("127.0.0.2")
                .build();
        ExtBookieInfo newInfo2 = ExtBookieInfo.builder()
                .address("127.0.0.3:3181")
                .group("default")
                .rack("/rack3")
                .hostname("127.0.0.3")
                .build();

        // 2. batch update
        admin.bookies().batchUpdateBookiesRackInfo(List.of(newInfo1, newInfo2));

        BookieInfo readInfo1 = admin.bookies().getBookieRackInfo(newInfo1.getAddress());
        assertEquals(newInfo1.getRack(), readInfo1.getRack());
        assertEquals(newInfo1.getHostname(), readInfo1.getHostname());

        BookieInfo readInfo2 = admin.bookies().getBookieRackInfo(newInfo2.getAddress());
        assertEquals(newInfo2.getRack(), readInfo2.getRack());
        assertEquals(newInfo2.getHostname(), readInfo2.getHostname());

        conf = admin.bookies().getBookiesRackInfo();
        assertEquals(1, conf.size());
    }

    @Test
    public void testDeleteBookieRackInfo() throws Exception {
        String bookie0 = "127.0.0.1:3181";
        String bookie1 = "127.0.0.2:3181";
        String bookie2 = "127.0.0.3:3181";
        String bookie3 = "127.0.0.4:3181";
        String group = "default";
        BookieInfo newInfo0 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.1")
                .build();
        BookieInfo newInfo1 = BookieInfo.builder()
                .rack("/rack2")
                .hostname("127.0.0.2")
                .build();
        BookieInfo newInfo2 = BookieInfo.builder()
                .rack("/rack3")
                .hostname("127.0.0.3")
                .build();
        BookieInfo newInfo3 = BookieInfo.builder()
                .rack("/rack4")
                .hostname("127.0.0.4")
                .build();
        admin.bookies().updateBookieRackInfo(bookie0, group, newInfo0);
        admin.bookies().updateBookieRackInfo(bookie1, group, newInfo1);
        admin.bookies().updateBookieRackInfo(bookie2, group, newInfo2);
        admin.bookies().updateBookieRackInfo(bookie3, group, newInfo3);
        BookieInfo readInfo0 = admin.bookies().getBookieRackInfo(bookie0);
        assertEquals(newInfo0, readInfo0);
        BookieInfo readInfo1 = admin.bookies().getBookieRackInfo(bookie1);
        assertEquals(newInfo1, readInfo1);
        BookieInfo readInfo2 = admin.bookies().getBookieRackInfo(bookie2);
        assertEquals(newInfo2, readInfo2);
        BookieInfo readInfo3 = admin.bookies().getBookieRackInfo(bookie3);
        assertEquals(newInfo3, readInfo3);

        // 1. delete
        admin.bookies().deleteBookieRackInfo(bookie0);
        try {
            admin.bookies().getBookieRackInfo(bookie0);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        readInfo1 = admin.bookies().getBookieRackInfo(bookie1);
        assertEquals(newInfo1, readInfo1);
        readInfo2 = admin.bookies().getBookieRackInfo(bookie2);
        assertEquals(newInfo2, readInfo2);
        readInfo3 = admin.bookies().getBookieRackInfo(bookie3);
        assertEquals(newInfo3, readInfo3);

        // 2. batch delete
        admin.bookies().batchDeleteBookiesRackInfo(List.of(bookie1, bookie2));
        try {
            admin.bookies().getBookieRackInfo(bookie1);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        try {
            admin.bookies().getBookieRackInfo(bookie2);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        readInfo3 = admin.bookies().getBookieRackInfo(bookie3);
        assertEquals(newInfo3, readInfo3);

        // 3. bookie does not exist
        try {
            admin.bookies().deleteBookieRackInfo(bookie1);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(404, e.getStatusCode());
        }
    }

    @Test
    public void testClearAllBookiesRackInfo() throws Exception {
        String bookie0 = "127.0.0.1:3181";
        String bookie1 = "127.0.0.2:3181";
        String bookie2 = "127.0.0.3:3181";
        String group = "default";
        BookieInfo newInfo0 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.1")
                .build();
        BookieInfo newInfo1 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.2")
                .build();
        BookieInfo newInfo2 = BookieInfo.builder()
                .rack("/rack1")
                .hostname("127.0.0.3")
                .build();
        admin.bookies().updateBookieRackInfo(bookie0, group, newInfo0);
        admin.bookies().updateBookieRackInfo(bookie1, group, newInfo1);
        admin.bookies().updateBookieRackInfo(bookie2, group, newInfo2);
        BookieInfo readInfo0 = admin.bookies().getBookieRackInfo(bookie0);
        assertEquals(newInfo0, readInfo0);
        BookieInfo readInfo1 = admin.bookies().getBookieRackInfo(bookie1);
        assertEquals(newInfo1, readInfo1);
        BookieInfo readInfo2 = admin.bookies().getBookieRackInfo(bookie2);
        assertEquals(newInfo2, readInfo2);

        admin.bookies().clearAllBookiesRackInfo();

        try {
            admin.bookies().getBookieRackInfo(bookie0);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        try {
            admin.bookies().getBookieRackInfo(bookie1);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        try {
            admin.bookies().getBookieRackInfo(bookie2);
            fail("should not reach here");
        } catch (PulsarAdminException pae) {
            assertEquals(404, pae.getStatusCode());
        }
        BookiesRackConfiguration conf = admin.bookies().getBookiesRackInfo();
        assertTrue(conf.isEmpty());
    }

}
