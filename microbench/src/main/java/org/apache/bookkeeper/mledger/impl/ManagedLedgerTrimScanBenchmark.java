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
package org.apache.bookkeeper.mledger.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.OffloadContext;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Isolates offloaded BookKeeper deletion candidate selection after retention has selected an ordinary deletion prefix.
 * Copies the original and optimized scan algorithms, including the offload predicate and result-list allocation.
 * Excludes retention checks, locking, executor scheduling, metadata updates, and BookKeeper operations.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
public class ManagedLedgerTrimScanBenchmark {

    private static final long DELETION_LAG_MILLIS = TimeUnit.HOURS.toMillis(1);

    @Param({"1000", "10000"})
    public int ledgerCount;

    @Param({"0", "50"})
    public int ordinaryDeletionPercent;

    @Param({"ABSENT", "NULL_LAG", "NEGATIVE_LAG", "ZERO_LAG", "POSITIVE_LAG"})
    public String policy;

    private Clock clock;
    private NavigableMap<Long, LedgerInfo> ledgers;
    private List<LedgerInfo> ledgersToDelete;
    private Optional<OffloadPolicies> optionalOffloadPolicies;

    @Setup
    public void setup() {
        clock = Clock.systemUTC();
        ledgers = new ConcurrentSkipListMap<>();
        ledgersToDelete = new ArrayList<>();
        int ordinaryDeletionCount = ledgerCount * ordinaryDeletionPercent / 100;
        long now = clock.millis();
        for (int i = 0; i < ledgerCount; i++) {
            // BookKeeper IDs need not be consecutive within one managed ledger.
            long ledgerId = 1000L + i * 3L;
            LedgerInfo ledger = new LedgerInfo().setLedgerId(ledgerId)
                    .setEntries(1000).setSize(1024 * 1024).setTimestamp(now - TimeUnit.DAYS.toMillis(3));
            if (i % 8 != 6) {
                OffloadContext offload = ledger.setOffloadContext();
                offload.setComplete(i % 8 != 4 && i != ledgerCount - 1);
                offload.setBookkeeperDeleted(i % 8 == 5);
                // Keep recent offloads safely below the positive lag throughout a normal benchmark run.
                offload.setTimestamp(i % 8 == 3 ? now - 1000 : now - TimeUnit.DAYS.toMillis(2));
            }
            ledgers.put(ledgerId, ledger);
            if (i < ordinaryDeletionCount) {
                ledgersToDelete.add(ledger);
            }
        }

        OffloadPoliciesImpl policies = new OffloadPoliciesImpl();
        switch (policy) {
            case "ABSENT" -> optionalOffloadPolicies = Optional.empty();
            case "NULL_LAG" -> {
                policies.setManagedLedgerOffloadDeletionLagInMillis(null);
                optionalOffloadPolicies = Optional.of(policies);
            }
            case "NEGATIVE_LAG" -> {
                policies.setManagedLedgerOffloadDeletionLagInMillis(-1L);
                optionalOffloadPolicies = Optional.of(policies);
            }
            case "ZERO_LAG" -> {
                policies.setManagedLedgerOffloadDeletionLagInMillis(0L);
                optionalOffloadPolicies = Optional.of(policies);
            }
            case "POSITIVE_LAG" -> {
                policies.setManagedLedgerOffloadDeletionLagInMillis(DELETION_LAG_MILLIS);
                optionalOffloadPolicies = Optional.of(policies);
            }
            default -> throw new IllegalArgumentException("Unknown offload policy: " + policy);
        }

        if (!baselineFullScanAndContains().equals(guardedTailScan())) {
            throw new IllegalStateException("Candidate selections disagree for " + policy
                    + ", ledgerCount=" + ledgerCount + ", ordinaryDeletionPercent=" + ordinaryDeletionPercent);
        }
    }

    @Benchmark
    public List<LedgerInfo> baselineFullScanAndContains() {
        List<LedgerInfo> offloadedLedgersToDelete = new ArrayList<>();
        for (LedgerInfo ledger : ledgers.values()) {
            if (isOffloadedNeedsDelete(ledger.getOffloadContext(), optionalOffloadPolicies)
                    && !ledgersToDelete.contains(ledger)) {
                offloadedLedgersToDelete.add(ledger);
            }
        }
        return offloadedLedgersToDelete;
    }

    @Benchmark
    public List<LedgerInfo> guardedTailScan() {
        List<LedgerInfo> offloadedLedgersToDelete = new ArrayList<>();
        Long offloadDeletionLag = optionalOffloadPolicies.isPresent()
                ? optionalOffloadPolicies.get().getManagedLedgerOffloadDeletionLagInMillis() : null;
        if (offloadDeletionLag != null && offloadDeletionLag >= 0) {
            Collection<LedgerInfo> remainingLedgers = ledgersToDelete.isEmpty() ? ledgers.values()
                    : ledgers.tailMap(ledgersToDelete.get(ledgersToDelete.size() - 1).getLedgerId(), false).values();
            for (LedgerInfo ledger : remainingLedgers) {
                if (isOffloadedNeedsDelete(ledger.getOffloadContext(), optionalOffloadPolicies)) {
                    offloadedLedgersToDelete.add(ledger);
                }
            }
        }
        return offloadedLedgersToDelete;
    }

    private boolean isOffloadedNeedsDelete(OffloadContext offload, Optional<OffloadPolicies> offloadPolicies) {
        long elapsedMs = clock.millis() - offload.getTimestamp();
        return offloadPolicies.filter(policies -> offload.isComplete() && !offload.isBookkeeperDeleted()
                && policies.getManagedLedgerOffloadDeletionLagInMillis() != null
                && policies.getManagedLedgerOffloadDeletionLagInMillis() >= 0
                && elapsedMs > policies.getManagedLedgerOffloadDeletionLagInMillis()).isPresent();
    }
}
