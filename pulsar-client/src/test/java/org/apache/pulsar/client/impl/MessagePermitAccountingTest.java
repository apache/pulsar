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
import org.apache.pulsar.client.impl.MessagePermitAccounting.Budget;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.testng.annotations.Test;

public class MessagePermitAccountingTest {

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
}
