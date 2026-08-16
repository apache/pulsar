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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;
import java.util.BitSet;
import java.util.SplittableRandom;
import org.apache.pulsar.client.impl.MessagePermitAccounting.Budget;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.testng.annotations.Test;

public class MessagePermitAccountingTest {

    private static final long RANDOM_SEED = 0x4915A17E5L;

    @Test
    public void testResolveExplicitMessagePermits() {
        CommandMessage command = new CommandMessage().setMessagePermits(3);
        command.addAckSet(0b100101L);

        assertEquals(MessagePermitAccounting.resolveForEarlyFailure(command), 3);
        assertEquals(MessagePermitAccounting.resolve(command, 10), 3);

        CommandMessage partialBatchWithOnePermit = new CommandMessage().setMessagePermits(1);
        partialBatchWithOnePermit.addAckSet(0b100L);
        assertEquals(MessagePermitAccounting.resolveForEarlyFailure(partialBatchWithOnePermit), 1);
        assertEquals(MessagePermitAccounting.resolve(partialBatchWithOnePermit, 10), 1);

        CommandMessage singleMessage = new CommandMessage().setMessagePermits(1);
        assertEquals(MessagePermitAccounting.resolveForEarlyFailure(singleMessage), 1);
        assertEquals(MessagePermitAccounting.resolve(singleMessage, 1), 1);
    }

    @Test
    public void testResolveLegacyMessagePermits() {
        CommandMessage partialBatch = new CommandMessage();
        partialBatch.addAckSet((1L << 1) | (1L << 4) | (1L << 63));

        assertEquals(MessagePermitAccounting.resolveForEarlyFailure(new CommandMessage()), 1);
        assertEquals(MessagePermitAccounting.resolveForEarlyFailure(partialBatch), 3);
        assertEquals(MessagePermitAccounting.resolve(partialBatch, 10), 2);
        assertEquals(MessagePermitAccounting.resolve(new CommandMessage(), 10), 10);
    }

    @Test
    public void testRejectInvalidExplicitMessagePermits() {
        CommandMessage zero = new CommandMessage().setMessagePermits(0);
        CommandMessage unsignedOverflow = new CommandMessage().setMessagePermits(-1);
        CommandMessage mismatch = new CommandMessage().setMessagePermits(2);
        mismatch.addAckSet(0b111L);
        CommandMessage onePermitMismatch = new CommandMessage().setMessagePermits(1);

        expectThrows(IllegalStateException.class, () -> MessagePermitAccounting.resolveForEarlyFailure(zero));
        expectThrows(IllegalStateException.class,
                () -> MessagePermitAccounting.resolveForEarlyFailure(unsignedOverflow));
        expectThrows(IllegalStateException.class, () -> MessagePermitAccounting.resolve(mismatch, 10));
        expectThrows(IllegalStateException.class, () -> MessagePermitAccounting.resolve(onePermitMismatch, 10));
    }

    @Test
    public void testBudgetTracksClaimsAndRestores() {
        Budget budget = new Budget(2);
        budget.claim();
        budget.restore();
        budget.claim();

        assertEquals(budget.drain(), 1);
        assertEquals(budget.drain(), 0);
        expectThrows(IllegalStateException.class, budget::claim);
    }

    @Test
    public void testRandomizedAckSetResolutionMatchesBoundedBitSetCardinality() {
        SplittableRandom random = new SplittableRandom(RANDOM_SEED);
        for (int testCase = 0; testCase < 10_000; testCase++) {
            int batchSize = random.nextInt(1, 513);
            int requiredWords = (batchSize + Long.SIZE - 1) / Long.SIZE;
            long[] ackSet = new long[requiredWords + random.nextInt(3)];
            for (int i = 0; i < ackSet.length; i++) {
                ackSet[i] = random.nextLong();
            }
            // Explicit message permits must be positive. Also guarantees coverage when random data happens to be zero.
            int requiredIndex = random.nextInt(batchSize);
            ackSet[requiredIndex / Long.SIZE] |= 1L << (requiredIndex % Long.SIZE);

            BitSet boundedAckSet = BitSet.valueOf(ackSet);
            boundedAckSet.clear(batchSize, Math.max(batchSize, boundedAckSet.length()));
            int expectedPermits = boundedAckSet.cardinality();
            int allAckSetPermits = BitSet.valueOf(ackSet).cardinality();
            String description = "seed=" + RANDOM_SEED + ", case=" + testCase + ", batchSize=" + batchSize;

            CommandMessage explicit = commandWithAckSet(ackSet).setMessagePermits(expectedPermits);
            assertEquals(MessagePermitAccounting.resolve(explicit, batchSize), expectedPermits, description);
            assertEquals(MessagePermitAccounting.resolveForEarlyFailure(explicit), expectedPermits, description);

            CommandMessage legacy = commandWithAckSet(ackSet);
            assertEquals(MessagePermitAccounting.resolve(legacy, batchSize), expectedPermits, description);
            assertEquals(MessagePermitAccounting.resolveForEarlyFailure(legacy), allAckSetPermits, description);
        }
    }

    @Test
    public void testRandomizedBudgetConservesPermitsAcrossClaimsAndRestores() {
        SplittableRandom random = new SplittableRandom(RANDOM_SEED);
        for (int testCase = 0; testCase < 10_000; testCase++) {
            int initialPermits = random.nextInt(1, 257);
            int modeledRemaining = initialPermits;
            int permanentlyClaimed = 0;
            Budget budget = new Budget(initialPermits);

            int operations = random.nextInt(1, initialPermits * 3 + 1);
            for (int operation = 0; operation < operations && modeledRemaining > 0; operation++) {
                if (random.nextBoolean()) {
                    budget.claim();
                    modeledRemaining--;
                    permanentlyClaimed++;
                } else {
                    budget.claim();
                    budget.restore();
                }
            }

            int returnedPermits = budget.drain();
            String description = "seed=" + RANDOM_SEED + ", case=" + testCase;
            assertEquals(returnedPermits, modeledRemaining, description);
            assertEquals(permanentlyClaimed + returnedPermits, initialPermits, description);
            assertEquals(budget.drain(), 0, description);
        }
    }

    private static CommandMessage commandWithAckSet(long[] ackSet) {
        CommandMessage command = new CommandMessage();
        for (long word : ackSet) {
            command.addAckSet(word);
        }
        return command;
    }
}
