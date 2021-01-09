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
package org.apache.pulsar.transaction.coordinator;

import com.google.common.collect.Sets;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.apache.pulsar.transaction.coordinator.util.TransactionUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit test {@link TxnStatus}.
 */
public class TxnStatusTest {

    @DataProvider(name = "statuses")
    public static Object[][] statuses() {
        return new Object[][] {
            {
                TxnStatus.OPEN,
                Sets.newHashSet(
                    TxnStatus.OPEN,
                    TxnStatus.COMMITTING,
                    TxnStatus.ABORTING
                ),
                Sets.newHashSet(
                    TxnStatus.COMMITTED,
                    TxnStatus.ABORTED
                )
            },
            {
                TxnStatus.COMMITTING,
                Sets.newHashSet(
                    TxnStatus.COMMITTING,
                    TxnStatus.COMMITTED
                ),
                Sets.newHashSet(
                    TxnStatus.OPEN,
                    TxnStatus.ABORTING,
                    TxnStatus.ABORTED
                )
            },
            {
                TxnStatus.COMMITTED,
                Sets.newHashSet(
                    TxnStatus.COMMITTED
                ),
                Sets.newHashSet(
                    TxnStatus.OPEN,
                    TxnStatus.COMMITTING,
                    TxnStatus.ABORTING,
                    TxnStatus.ABORTED
                )
            },
            {
                TxnStatus.ABORTING,
                Sets.newHashSet(
                    TxnStatus.ABORTING,
                    TxnStatus.ABORTED
                ),
                Sets.newHashSet(
                    TxnStatus.OPEN,
                    TxnStatus.COMMITTING,
                    TxnStatus.COMMITTED
                )
            },
            {
                TxnStatus.ABORTED,
                Sets.newHashSet(
                    TxnStatus.ABORTED
                ),
                Sets.newHashSet(
                    TxnStatus.OPEN,
                    TxnStatus.COMMITTING,
                    TxnStatus.COMMITTED,
                    TxnStatus.ABORTING
                )
            },
        };
    }

    @Test(dataProvider = "statuses")
    public void testTxnStatusTransition(TxnStatus status,
                                        Set<TxnStatus> statusesCanTransitionTo,
                                        Set<TxnStatus> statusesCanNotTransactionTo) {
        statusesCanTransitionTo.forEach(newStatus -> {
            assertTrue(
                    TransactionUtil.canTransitionTo(status, newStatus),
                "Status `" + status + "` should be able to transition to `" + newStatus + "`"
            );
        });
        statusesCanNotTransactionTo.forEach(newStatus -> {
            assertFalse(
                    TransactionUtil.canTransitionTo(status, newStatus),
                "Status `" + status + "` should NOT be able to transition to `" + newStatus + "`"
            );
        });
    }

}
