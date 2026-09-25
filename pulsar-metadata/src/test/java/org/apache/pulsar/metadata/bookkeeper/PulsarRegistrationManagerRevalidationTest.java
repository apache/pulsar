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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.coordination.impl.ResourceLockImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests of the session-loss driven bookie registration re-validation in
 * {@link PulsarRegistrationManager}, running the real lock stack
 * (CoordinationServiceImpl / LockManagerImpl / ResourceLockImpl) on top of
 * {@link FakeOxiaSessionMetadataStore}, which models the oxia store identity, session and
 * version semantics.
 *
 * <p>The test identifiers (T1..T11) refer to the test plan of the design document.
 */
public class PulsarRegistrationManagerRevalidationTest {

    private static final String LEDGERS_ROOT = "/ledgers-revalidation-test";
    private static final String SELF_IDENTITY = "self-client-identity";
    private static final String FOREIGN_IDENTITY = "foreign-client-identity";

    private static final String ATTEMPT = "Re-validating bookie registration";
    private static final String SUCCESS = "registration re-validated";
    private static final String ESCALATE = "registration is held by another owner";
    private static final String RING_START = "Metadata store session was lost";
    private static final String REACQUIRE = "is no longer valid, re-acquiring";
    private static final String STALE_HOLDER_WAIT = "currently owned by a stale session with the same value";
    private static final String NON_EPHEMERAL_OCCUPANT = "occupied by a non-ephemeral node";
    private static final String LOCK_MARKED_EXPIRED = "Marked as expired";
    private static final String REVALIDATED_LOCK = "Successfully revalidated the lock";

    private static final AtomicLong APPENDER_SEQUENCE = new AtomicLong();

    private final BookieId bookieId = BookieId.parse("bookie-1:3181");
    private final String regPath = LEDGERS_ROOT + "/available/" + bookieId;
    private final String regPathReadOnly = LEDGERS_ROOT + "/available/readonly/" + bookieId;

    private FakeOxiaSessionMetadataStore store;
    private PulsarRegistrationManager registrationManager;
    private CapturingAppender registrationManagerLogs;
    private CapturingAppender resourceLockLogs;
    private final AtomicInteger expiredCount = new AtomicInteger();

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        expiredCount.set(0);
        store = new FakeOxiaSessionMetadataStore(SELF_IDENTITY);
        registrationManager = new PulsarRegistrationManager(store, LEDGERS_ROOT, new ServerConfiguration());
        registrationManager.addRegistrationListener(expiredCount::incrementAndGet);
        registrationManagerLogs = attachAppender(PulsarRegistrationManager.class);
        resourceLockLogs = attachAppender(ResourceLockImpl.class);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        detachAppender(PulsarRegistrationManager.class, registrationManagerLogs);
        detachAppender(ResourceLockImpl.class, resourceLockLogs);
        if (registrationManager != null) {
            registrationManager.close();
            registrationManager = null;
        }
        if (store != null) {
            store.close();
            store = null;
        }
    }

    private CapturingAppender attachAppender(Class<?> clazz) {
        Logger logger = (Logger) LogManager.getLogger(clazz);
        CapturingAppender appender = new CapturingAppender(
                "capture-" + clazz.getSimpleName() + "-" + APPENDER_SEQUENCE.incrementAndGet());
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        return appender;
    }

    private void detachAppender(Class<?> clazz, CapturingAppender appender) {
        if (appender != null) {
            ((Logger) LogManager.getLogger(clazz)).removeAppender(appender);
            appender.stop();
        }
    }

    private long attempts() {
        return registrationManagerLogs.countOf(ATTEMPT);
    }

    private long successes() {
        return registrationManagerLogs.countOf(SUCCESS);
    }

    private long escalations() {
        return registrationManagerLogs.countOf(ESCALATE);
    }

    /** Waits longer than the maximum backoff to observe that no further ring ticks happen. */
    private void assertRingIsQuiet() throws InterruptedException {
        long attemptsBefore = attempts();
        long putsBefore = store.getSuccessfulEphemeralPuts(regPath);
        Thread.sleep(6000);
        assertEquals(attempts(), attemptsBefore, "revalidation attempts kept happening");
        assertEquals(store.getSuccessfulEphemeralPuts(regPath), putsBefore, "registration writes kept happening");
    }

    private static BookieServiceInfo bookieServiceInfo() {
        return new BookieServiceInfo(Map.of("authName", "test"), List.of(new BookieServiceInfo.Endpoint(
                "endpoint-1", 3181, "bookie-1", "bookie", List.of(), List.of())));
    }

    private static BookieServiceInfo differentBookieServiceInfo() {
        return new BookieServiceInfo(Map.of("authName", "test"), List.of(new BookieServiceInfo.Endpoint(
                "endpoint-2", 3182, "bookie-2", "bookie", List.of(), List.of())));
    }

    /**
     * T1: a registration lost with the session is re-created by the ring, with a success log,
     * and the ring stops.
     */
    @Test
    public void revalidationRecreatesRegistrationAfterSessionPurge() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        assertTrue(store.getRecord(regPath).isPresent());
        long deadSession = store.getRecordSession(regPath);
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);

        // The oxia restart: the session dies and the server purges the ephemeral registration.
        store.expireSession(true);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "no success log was emitted"));
        assertTrue(attempts() >= 1, "no attempt log was emitted");

        Optional<FakeOxiaSessionMetadataStore.Record> record = store.getRecord(regPath);
        assertTrue(record.isPresent(), "registration was not re-created");
        assertTrue(store.isRecordCreatedBySelf(regPath), "re-created registration is not owned by self");
        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAtRegistration + 1,
                "the registration was not really re-written");
        assertTrue(store.getRecordSession(regPath) != deadSession,
                "re-created registration is still bound to the dead session");
        assertEquals(expiredCount.get(), 0);
        assertRingIsQuiet();
    }

    /**
     * T2: while the store is unavailable the ring retries with backoff and nothing converges;
     * once the store is available again it converges.
     */
    @Test
    public void revalidationRetriesThroughStoreUnavailability() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);

        store.setUnavailable(true);
        store.expireSession(true);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(attempts() >= 2, "the ring is not retrying"));
        assertFalse(store.getRecord(regPath).isPresent(), "registration re-created while store unavailable");
        assertEquals(successes(), 0);
        assertEquals(expiredCount.get(), 0);

        store.setUnavailable(false);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "no convergence after recovery"));
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath));
        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAtRegistration + 1,
                "the registration was not really re-written after recovery");
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the re-created registration is not bound to the live session");
        assertEquals(expiredCount.get(), 0);
        assertRingIsQuiet();
    }

    /**
     * T3 (round3 revision): in the server-side grace window the record still exists and
     * belongs to our identity with the same value; the ring must still perform a real put
     * that rebinds the record to the new session, and then stop.
     */
    @Test
    public void graceWindowRevalidationPerformsRealWrite() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);
        long deadSession = store.getRecordSession(regPath);
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);

        // The session dies, but the server-side grace window keeps the record in place,
        // still owned by the dead session and by our own client identity.
        store.expireSession(false);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "no success log was emitted"));

        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAtRegistration + 1,
                "the grace window converged without a new real registration put");
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath));
        assertTrue(store.getRecordSession(regPath) != deadSession,
                "the record was adopted without being rebound to the new session");
        assertEquals(expiredCount.get(), 0);
        assertRingIsQuiet();
    }

    /**
     * T3b: an invisible version substitution (a zombie write: same identity, same value,
     * still owned by the dead session) must not fool the ring into a zero-write success: a
     * real write happens, the record is rebound to the live session, and the later ghost
     * sweep of the dead session leaves it alone.
     */
    @Test
    public void revalidationWritesThroughInvisibleVersionBumps() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);
        long deadSession = store.getRecordSession(regPath);
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);

        // The session dies in the grace window and a zombie write bumps the record version
        // invisibly, still owned by the dead session.
        store.expireSession(false);
        store.invisibleVersionBump(regPath);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "no success log was emitted"));

        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAtRegistration + 1,
                "the ring converged without a real registration write");
        assertTrue(store.getRecord(regPath).isPresent());
        long liveSession = store.getRecordSession(regPath);
        assertTrue(liveSession != deadSession, "the record is still bound to the dead session");

        // The ghost sweep of the dead session must not touch the re-bound record.
        store.sweepSession(deadSession);
        assertTrue(store.getRecord(regPath).isPresent(), "the record died with the swept dead session");
        assertEquals(store.getRecordSession(regPath), liveSession);
        assertEquals(expiredCount.get(), 0);
    }

    /**
     * T3c: when an event-driven revalidation adopts the record first (zero write, handle
     * version synced), the ring tick must still perform its own write, so the final record
     * ends up on the live session instead of trusting the adopted state.
     */
    @Test
    public void eventDrivenAdoptThenRingTickKeepsRecordOnLiveSession() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);

        // Hold the SessionLost dispatch, so that the event-driven revalidation (a KEY_DELETED
        // notification) adopts the record first and the ring tick runs afterwards.
        CountDownLatch sessionEventGate = new CountDownLatch(1);
        store.holdSessionEvents(sessionEventGate);
        store.expireSession(false);
        store.invisibleVersionBump(regPath);
        store.fireDeletedNotification(regPath);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(resourceLockLogs.countOf(REVALIDATED_LOCK) >= 1,
                        "the event-driven revalidation did not adopt the record"));

        sessionEventGate.countDown();

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the ring did not converge"));

        assertTrue(store.getRecord(regPath).isPresent());
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the record was left on a session other than the live one");
        assertEquals(expiredCount.get(), 0);
    }

    /**
     * T4: a foreign owner holding a different value is only reported after three consecutive
     * confirmations; the BadVersion/LockBusy observations before that never escalate.
     */
    @Test
    public void escalatesAfterThreeConfirmedForeignConflicts() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        // A different bookie process takes over the registration with a different value.
        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, differentBookieServiceInfo()));

        store.expireSession(true);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(expiredCount.get(), 1, "listener was not notified"));

        assertEquals(escalations(), 1);
        assertTrue(attempts() >= 3, "escalation happened before three confirmations");
        assertTrue(store.getRecord(regPath).isPresent(), "the foreign record must be left untouched");
        assertFalse(store.isRecordCreatedBySelf(regPath));

        assertRingIsQuiet();
        assertEquals(expiredCount.get(), 1);
    }

    /**
     * T4 extension: the escalation counter only counts consecutive confirmations. Two
     * foreign different-value observations followed by an interruption, here the holder
     * switching to the same value, reset the counter and the ring converges by trampling,
     * without ever escalating.
     */
    @Test
    public void foreignConflictCounterResetsOnOtherObservations() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);

        // A different owner with a different value holds the path.
        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, differentBookieServiceInfo()));

        store.expireSession(true);

        // Wait for two confirmations, then replace the holder with a stale copy of this same
        // bookie (same value) before a third confirmation could happen.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(attempts() >= 2, "expected at least two attempts"));
        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, info));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the registration did not converge"));

        assertTrue(registrationManagerLogs.countOf(STALE_HOLDER_WAIT) >= 1,
                "the disambiguation never observed the same-value holder");
        assertEquals(expiredCount.get(), 0, "an interrupted conflict streak escalated");
        assertEquals(escalations(), 0);
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath), "the registration was not trampled back");
    }

    /**
     * T5 (round3 revision): a foreign holder with the same value (a zombie copy of this same
     * bookie) is trampled with the versioned write; a write race inside the read-to-write
     * window only makes the ring retry. It never escalates and never kills the bookie, and
     * the reacquire leg converges with the adopt path, without any extra registration put.
     */
    @Test
    public void foreignHolderWithSameValueNeverEscalates() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);

        // The zombie copy of this bookie holds the path with the same value.
        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, info));

        store.expireSession(true);

        // The first write attempt loses a read/write race and only retries.
        store.failNextPutsWithBadVersion(1);
        await().atMost(Duration.ofSeconds(4))
                .untilAsserted(() -> assertTrue(attempts() >= 1));

        // The next attempt tramples the stale record with the versioned write and succeeds.
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the stale record was not re-acquired"));

        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath), "the registration was not trampled back");
        // Exactly one registration put (the trampling write): the reacquire converged through
        // the adopt path without writing again.
        assertEquals(store.getSuccessfulEphemeralPuts(regPath), putsAtRegistration + 1,
                "the reacquire leg wrote unexpectedly");
        assertEquals(expiredCount.get(), 0, "a same-value conflict must never escalate");
        assertEquals(escalations(), 0);
        assertRingIsQuiet();
    }

    /**
     * T6: close() racing an in-flight revalidation attempt must not throw, must not produce a
     * late success log, and the ring must stay silent afterwards. A write already in flight
     * when close() released the registrations is cleaned up by the manager; a write landing
     * even later through the coordination layer's own queued revalidation is purged when the
     * bookie's store session dies at shutdown, which is modeled with an explicit session
     * expiry at the end.
     */
    @Test
    public void closeDuringInFlightRevalidationDiscardsLateCompletion() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        store.setPutDelayMillis(3000);
        store.expireSession(true);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(attempts() >= 1));

        registrationManager.close();

        // Let the delayed completions land after the close.
        Thread.sleep(4500);

        assertEquals(successes(), 0, "a late completion produced a success log after close");
        long attemptsAfterClose = attempts();
        Thread.sleep(2000);
        assertEquals(attempts(), attemptsAfterClose, "the ring kept attempting after close");
        assertEquals(expiredCount.get(), 0);

        // The bookie shutting down closes its metadata store: the session dies and purges
        // any record that a late write might have left behind.
        store.expireSession(true);
        assertFalse(store.getRecord(regPath).isPresent(), "a registration survived the shutdown purge");
    }

    /**
     * T7: a session loss before the first registration has nothing to revalidate and starts
     * no ring; a later registration works normally.
     */
    @Test
    public void sessionLossBeforeFirstRegistrationStartsNoRing() throws Exception {
        store.fireSessionLost();

        Thread.sleep(1500);
        assertEquals(registrationManagerLogs.countOf(RING_START), 0);
        assertEquals(attempts(), 0);

        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath));
        assertEquals(expiredCount.get(), 0);
    }

    /**
     * T8: two SessionLost events while a ring is already active start a single ring.
     */
    @Test
    public void doubleSessionLostRunsSingleRevalidationRing() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);

        // Keep the ring active in its retry loop while the second event arrives.
        store.setUnavailable(true);
        store.expireSession(true);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(attempts() >= 1));

        store.fireSessionLost();
        Thread.sleep(1500);
        assertEquals(registrationManagerLogs.countOf(RING_START), 1, "more than one ring was started");

        store.setUnavailable(false);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertEquals(successes(), 1));
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAtRegistration + 1,
                "the registration was not really re-written");
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the re-created registration is not bound to the live session");
        assertEquals(expiredCount.get(), 0);
        assertRingIsQuiet();
    }

    /**
     * T8 extension: a session lost after a previous ring fully finished and went quiet starts a
     * new ring, instead of being swallowed by any leftover loop bookkeeping.
     */
    @Test
    public void sessionLossAfterTheRingFinishedStartsANewRing() throws Exception {
        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());

        store.expireSession(true);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the first ring did not converge"));
        assertRingIsQuiet();
        long putsAfterFirstRing = store.getSuccessfulEphemeralPuts(regPath);
        assertEquals(registrationManagerLogs.countOf(RING_START), 1);

        store.expireSession(true);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 2, "the second ring did not converge"));

        assertEquals(registrationManagerLogs.countOf(RING_START), 2);
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath));
        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAfterFirstRing + 1,
                "the registration was not really re-written by the second ring");
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the re-created registration is not bound to the live session");
        assertEquals(expiredCount.get(), 0);
        assertRingIsQuiet();
    }

    /**
     * T9a: a readonly conversion interleaved with an in-flight ring attempt must not leave
     * two records: the stale in-flight write that resurrects the writable registration after
     * the conversion is cleaned up.
     */
    @Test
    public void readonlyConversionDuringInFlightRevalidationKeepsSingleRecord() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);

        store.setPutDelayMillis(1000);
        store.expireSession(true);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(attempts() >= 1));

        // The bookie converts to readonly while the ring attempt is still in flight.
        long start = System.nanoTime();
        registrationManager.registerBookie(bookieId, true, info);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // The public call blocked until the delayed registration put completed, on the
        // mutation executor thread.
        assertTrue(elapsedMillis >= 700, "registerBookie did not block until completion");
        assertTrue(store.getLastPutThread(regPathReadOnly) != null
                        && store.getLastPutThread(regPathReadOnly).startsWith("bookie-registration-mutation"),
                "the registration mutation did not run on the mutation executor");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertFalse(store.getRecord(regPath).isPresent(),
                        "the writable registration was resurrected and left behind"));
        assertTrue(store.getRecord(regPathReadOnly).isPresent(), "the readonly registration is missing");
        assertTrue(store.isRecordCreatedBySelf(regPathReadOnly));
        assertEquals(successes(), 0, "a stale completion produced a success log");
        assertEquals(expiredCount.get(), 0);
    }

    /**
     * T9b: when the readonly conversion fully completes before the ring's first tick (the
     * SessionLost event is still in dispatch), the ring exits without resurrecting the
     * released writable registration.
     */
    @Test
    public void readonlyConversionBeforeRingTickDoesNotResurrectWritableRegistration() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);

        // Hold the SessionLost dispatch until the conversion has completed.
        CountDownLatch sessionEventGate = new CountDownLatch(1);
        store.holdSessionEvents(sessionEventGate);
        store.expireSession(true);

        registrationManager.registerBookie(bookieId, true, info);
        assertTrue(store.getRecord(regPathReadOnly).isPresent());
        assertFalse(store.getRecord(regPath).isPresent());
        long writablePutsBeforeRing = store.getSuccessfulEphemeralPuts(regPath);

        sessionEventGate.countDown();

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the readonly registration was not revalidated"));

        assertEquals(store.getSuccessfulEphemeralPuts(regPath), writablePutsBeforeRing,
                "the writable registration was resurrected after the conversion");
        assertFalse(store.getRecord(regPath).isPresent());
        assertTrue(store.getRecord(regPathReadOnly).isPresent());
        assertEquals(expiredCount.get(), 0);
    }

    /**
     * T9c: the public registerBookie is a submit-and-block adapter: the caller thread waits
     * for the completion and failures propagate synchronously.
     */
    @Test
    public void registerBookieBlocksUntilCompletionAndPropagatesFailure() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);
        assertTrue(store.getLastPutThread(regPath).startsWith("bookie-registration-mutation"),
                "the registration mutation did not run on the mutation executor");

        // A failing registration propagates to the calling thread.
        store.setUnavailable(true);
        try {
            registrationManager.registerBookie(BookieId.parse("bookie-failing:3181"), false, info);
            fail("registerBookie did not propagate the failure");
        } catch (BookieException expected) {
            // expected
        } finally {
            store.setUnavailable(false);
        }
    }

    /**
     * T10: the ring's revalidation put wakes up a new session (SessionLost followed by
     * SessionReestablished) without ever blocking the store event executor.
     */
    @Test
    public void revalidationWakesNewSessionWithoutBlockingEventExecutor() throws Exception {
        List<SessionEvent> sessionEvents = new CopyOnWriteArrayList<>();
        store.registerSessionListener(sessionEvents::add);

        registrationManager.registerBookie(bookieId, false, bookieServiceInfo());
        long putsAtRegistration = store.getSuccessfulEphemeralPuts(regPath);
        store.setPutDelayMillis(300);
        store.expireSession(true);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(attempts() >= 1));

        // While the ring is actively retrying, the store event executor must stay responsive:
        // the session listener callback only schedules work, it never does I/O on it.
        ExecutorService eventExecutor = store.getEventExecutor();
        CompletableFuture<Long> probe = new CompletableFuture<>();
        long probeStart = System.nanoTime();
        eventExecutor.execute(() -> probe.complete(System.nanoTime()));
        long probeLatencyMillis = TimeUnit.NANOSECONDS.toMillis(probe.get(2, TimeUnit.SECONDS) - probeStart);
        assertTrue(probeLatencyMillis < 1000, "the event executor was blocked for " + probeLatencyMillis + "ms");

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    int lost = sessionEvents.indexOf(SessionEvent.SessionLost);
                    int reestablished = sessionEvents.indexOf(SessionEvent.SessionReestablished);
                    assertTrue(lost >= 0, "SessionLost was not delivered");
                    assertTrue(reestablished > lost, "the revalidation put did not wake a new session");
                });

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1));
        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath));
        assertTrue(store.getSuccessfulEphemeralPuts(regPath) >= putsAtRegistration + 1,
                "the registration was not really re-written");
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the re-created registration is not bound to the live session");
        assertEquals(expiredCount.get(), 0);
    }

    /**
     * T11: when an event-driven revalidation loses a race and the ring's lock handle is
     * marked dead (state Released), the ring still recovers: its write tramples the stale
     * record and the reacquire leg swaps the dead handle for a fresh one, without exiting
     * the ring and without going through the public blocking adapter.
     */
    @Test
    public void deadLockHandleFallsBackToInternalReacquire() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);

        // A zombie copy of this bookie holds the path with the same value, and the
        // event-driven revalidation (a KEY_DELETED notification) loses the delete/re-put
        // race: the handle is marked dead.
        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, info));
        store.failNextDeletesWithBadVersion(1);
        store.fireDeletedNotification(regPath);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(resourceLockLogs.countOf(LOCK_MARKED_EXPIRED) >= 1,
                        "the lock handle was not marked dead by the event-driven revalidation"));

        // The session loss starts the ring: the write tramples the stale record and the
        // reacquire leg swaps the dead handle for a fresh one.
        store.expireSession(true);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(registrationManagerLogs.countOf(REACQUIRE) >= 1,
                        "the dead handle was not replaced by the internal re-acquire"));
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the re-acquire did not converge"));

        assertTrue(store.getRecord(regPath).isPresent());
        assertTrue(store.isRecordCreatedBySelf(regPath), "the registration was not recovered by self");
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the recovered registration is not bound to the live session");
        assertEquals(expiredCount.get(), 0);
        assertEquals(escalations(), 0);
        assertRingIsQuiet();
    }

    /**
     * Review item 6/6: a non-ephemeral node occupying the registration path is never written,
     * because a put would silently convert the persistent node into an ephemeral one. It is
     * treated as a registration conflict and escalates after the consecutive confirmations.
     */
    @Test
    public void nonEphemeralOccupantIsNeverWrittenAndEscalates() throws Exception {
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);

        // A persistent node is mistakenly created at the registration path by a foreign
        // identity, overwriting our ephemeral record.
        store.putForeignRecord(regPath, FOREIGN_IDENTITY,
                BookieServiceInfoSerde.INSTANCE.serialize(regPath, differentBookieServiceInfo()), false);
        long occupiedVersion = store.getRecordVersion(regPath);

        store.expireSession(true);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(expiredCount.get(), 1, "listener was not notified"));
        assertEquals(escalations(), 1);
        assertTrue(attempts() >= 3, "escalation happened before three confirmations");
        assertTrue(registrationManagerLogs.countOf(NON_EPHEMERAL_OCCUPANT) >= 1,
                "the non-ephemeral occupant was not reported");

        // The persistent node was never written: same version, still non-ephemeral.
        Optional<FakeOxiaSessionMetadataStore.Record> record = store.getRecord(regPath);
        assertTrue(record.isPresent(), "the persistent node disappeared");
        assertFalse(record.get().isEphemeral(), "the persistent node was converted to ephemeral");
        assertEquals(record.get().getVersion(), occupiedVersion, "the persistent node was written");
        assertEquals(expiredCount.get(), 1);
    }

    /**
     * T-zk-ownership: under zk ownership semantics (a put never transfers the ephemeral
     * ownership, only a delete-and-recreate does), the final record ends up owned by the
     * live session: the ownership transfer is carried by the reacquire leg, which is why
     * the ring only stops after the reacquire completed.
     */
    @Test
    public void zkOwnershipTransferHappensThroughTheReacquireLeg() throws Exception {
        store.setZkOwnershipSemantics(true);
        BookieServiceInfo info = bookieServiceInfo();
        registrationManager.registerBookie(bookieId, false, info);
        long deadSession = store.getRecordSession(regPath);

        // The grace window keeps the record owned by the dead session, and under zk
        // semantics the versioned write alone cannot move that ownership.
        store.expireSession(false);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertTrue(successes() >= 1, "the ring did not converge"));

        assertTrue(store.getRecord(regPath).isPresent());
        Long liveSession = store.getCurrentSessionId();
        assertTrue(liveSession != null && liveSession != deadSession);
        assertEquals(store.getRecordSession(regPath), (long) liveSession,
                "the registration record is still owned by the dead session");
        assertTrue(store.isRecordCreatedBySelf(regPath));

        // The ghost sweep of the dead session must not touch the re-owned record.
        store.sweepSession(deadSession);
        assertTrue(store.getRecord(regPath).isPresent(), "the record still dies with the dead session");
        assertEquals(expiredCount.get(), 0);
    }

    /** Collects the formatted messages of a single log4j2 logger. */
    private static final class CapturingAppender extends AbstractAppender {

        private final List<String> messages = new CopyOnWriteArrayList<>();

        private CapturingAppender(String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        long countOf(String fragment) {
            return messages.stream().filter(message -> message.contains(fragment)).count();
        }
    }
}
