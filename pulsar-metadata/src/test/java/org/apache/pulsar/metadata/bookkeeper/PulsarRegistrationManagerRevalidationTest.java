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
package org.apache.pulsar.metadata.bookkeeper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.net.BookieId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests of the bookie registration recovery in {@link PulsarRegistrationManager}: the
 * registrations are healed by the coordination layer's lock revalidation after a session
 * loss, and the manager maps the expiry of a registration lock to the bookkeeper
 * registration-expired contract. Runs the real lock stack (CoordinationServiceImpl /
 * LockManagerImpl / ResourceLockImpl) on top of {@link FakeOxiaSessionMetadataStore}, which
 * models the oxia store identity, session and version semantics.
 */
public class PulsarRegistrationManagerRevalidationTest {

    private static final String LEDGERS_ROOT = "/ledgers-revalidation-test";
    private static final String SELF_IDENTITY = "self-client-identity";
    private static final String FOREIGN_IDENTITY = "foreign-client-identity";

    private final BookieId bookieId = BookieId.parse("bookie-1:3181");
    private final String regPath = LEDGERS_ROOT + "/available/" + bookieId;
    private final String regPathReadOnly = LEDGERS_ROOT + "/available/readonly/" + bookieId;

    private FakeOxiaSessionMetadataStore store;
    private PulsarRegistrationManager registrationManager;
    private final AtomicInteger expiredCount = new AtomicInteger();

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        expiredCount.set(0);
        store = new FakeOxiaSessionMetadataStore(SELF_IDENTITY);
        registrationManager = new PulsarRegistrationManager(store, LEDGERS_ROOT, new ServerConfiguration());
        registrationManager.addRegistrationListener(expiredCount::incrementAndGet);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (registrationManager != null) {
            registrationManager.close();
            registrationManager = null;
        }
        if (store != null) {
            store.close();
            store = null;
        }
    }

    private static BookieServiceInfo bookieServiceInfo() {
        return new BookieServiceInfo(Map.of("authName", "test"), List.of(new BookieServiceInfo.Endpoint(
                "endpoint-1", 3181, "bookie-1", "bookie", List.of(), List.of())));
    }

    private static BookieServiceInfo differentBookieServiceInfo() {
        return new BookieServiceInfo(Map.of("authName", "test"), List.of(new BookieServiceInfo.Endpoint(
                "endpoint-2", 3182, "bookie-2", "bookie", List.of(), List.of())));
    }

    @Test
    public void sweptRegistrationIsRecreatedOnTheLiveSessionAfterReestablishment() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        assertTrue(store.getRecord(regPath).isPresent());

        store.expireSession(true);
        assertFalse(store.getRecord(regPath).isPresent());

        // The session watcher reports the fresh session: the lock layer revalidates every
        // tracked lock and re-creates the swept registration on the live session, without
        // ever driving the bookie into a re-registration.
        store.fireSessionReestablished();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertTrue(store.getRecord(regPath).isPresent());
            assertEquals(store.getRecordSession(regPath), (long) store.getCurrentSessionId());
        });
        assertEquals(expiredCount.get(), 0);
    }

    @Test
    public void zombieRecordInTheGraceWindowIsReboundToTheLiveSession() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        long deadSession = store.getRecordSession(regPath);

        // The session dies but the record survives in the server-side grace window. On the
        // oxia backend the record still reads as created by this client identity, so the
        // revalidation adopts it — and the adoption re-writes the record, which re-binds it
        // to the live session.
        store.expireSession(false);
        assertTrue(store.getRecord(regPath).isPresent());

        store.fireSessionReestablished();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertTrue(store.getRecord(regPath).isPresent());
            Long liveSession = store.getCurrentSessionId();
            assertTrue(liveSession != null && liveSession != deadSession);
            assertEquals(store.getRecordSession(regPath), (long) liveSession);
        });
        assertEquals(expiredCount.get(), 0);
    }

    @Test
    public void zombieRecordUnderZkOwnershipSemanticsIsRecreatedOnTheLiveSession() throws Exception {
        store.setZkOwnershipSemantics(true);

        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        long deadSession = store.getRecordSession(regPath);

        store.expireSession(false);
        assertTrue(store.getRecord(regPath).isPresent());

        store.fireSessionReestablished();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertTrue(store.getRecord(regPath).isPresent());
            Long liveSession = store.getCurrentSessionId();
            assertTrue(liveSession != null && liveSession != deadSession);
            assertEquals(store.getRecordSession(regPath), (long) liveSession);
        });
        assertEquals(expiredCount.get(), 0);
    }

    @Test
    public void foreignDifferentValueHolderExpiresTheRegistration() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, differentBookieServiceInfo()));

        store.fireSessionReestablished();

        await().atMost(5, SECONDS).until(() -> expiredCount.get() >= 1);
        assertEquals(expiredCount.get(), 1);
    }

    @Test
    public void voluntaryUnregisterDoesNotNotifyTheListeners() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        registrationManager.unregisterBookie(bookieId, false);

        assertFalse(store.getRecord(regPath).isPresent());
        // The completion of the voluntary release must not drive the bookie into a
        // re-registration of what was just torn down.
        await().during(1, SECONDS).atMost(2, SECONDS)
                .untilAsserted(() -> assertEquals(expiredCount.get(), 0));
    }

    @Test
    public void readonlyConversionKeepsASingleRegistrationWithoutNotifying() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        registrationManager.registerBookie(bookieId, true, bookieServiceInfo());

        assertFalse(store.getRecord(regPath).isPresent());
        assertTrue(store.getRecord(regPathReadOnly).isPresent());
        assertEquals(expiredCount.get(), 0);
    }

    @Test
    public void failedUnregisterCanBeRetried() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        store.setUnavailable(true);
        assertThrows(BookieException.MetadataStoreException.class,
                () -> registrationManager.unregisterBookie(bookieId, false));
        // The handle stayed tracked: the registration record is still there and the failure
        // did not count as an expiry.
        assertTrue(store.getRecord(regPath).isPresent());
        assertEquals(expiredCount.get(), 0);

        store.setUnavailable(false);
        registrationManager.unregisterBookie(bookieId, false);

        assertFalse(store.getRecord(regPath).isPresent());
        assertEquals(expiredCount.get(), 0);
    }

    @Test
    public void unreleasableLockIsCleanedUpBeforeTheUnregisterReturns() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        // The release delete hits a version race: the record is then removed directly, and
        // the cleanup must have completed by the time the unregister returns.
        store.failNextDeletesWithBadVersion(1);

        registrationManager.unregisterBookie(bookieId, false);

        assertFalse(store.getRecord(regPath).isPresent());
        assertEquals(expiredCount.get(), 0);
    }

    @Test
    public void closeReleasesTheRegistrations() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        registrationManager.registerBookie(bookieId, true, bookieServiceInfo());

        registrationManager.close();

        assertFalse(store.getRecord(regPath).isPresent());
        assertFalse(store.getRecord(regPathReadOnly).isPresent());
    }
}
