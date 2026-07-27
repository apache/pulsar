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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.testng.annotations.Test;

/**
 * Roundtrip tests for the {@code scalable} routing flag on the transaction commands (PIP-473
 * coexistence): a v5 client sets it so the broker routes the command to the metadata-store
 * coordinator; a v4 client omits it and routes to the legacy coordinator. Each command is encoded,
 * the serialized wire frame is reparsed, and the flag is checked in both states.
 */
public class CommandsScalableTxnFlagTest {

    private static BaseCommand parseFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4); // total size
            int cmdSize = (int) frame.readUnsignedInt();
            BaseCommand cmd = new BaseCommand();
            cmd.parseFrom(frame, cmdSize);
            cmd.materialize();
            return cmd;
        } finally {
            frame.release();
        }
    }

    @Test
    public void tcClientConnectCarriesScalable() {
        assertTrue(parseFrame(Commands.newTcClientConnectRequest(1L, 2L, true))
                .getTcClientConnectRequest().isScalable());
        assertFalse(parseFrame(Commands.newTcClientConnectRequest(1L, 2L))
                .getTcClientConnectRequest().isScalable());
    }

    @Test
    public void newTxnCarriesScalable() {
        assertTrue(parseFrame(Commands.newTxn(0L, 1L, 60_000L, true)).getNewTxn().isScalable());
        assertFalse(parseFrame(Commands.newTxn(0L, 1L, 60_000L)).getNewTxn().isScalable());
    }

    @Test
    public void endTxnCarriesScalable() {
        assertTrue(Commands.newEndTxn(1L, 2L, 0L, TxnAction.COMMIT, true).getEndTxn().isScalable());
        assertFalse(Commands.newEndTxn(1L, 2L, 0L, TxnAction.COMMIT).getEndTxn().isScalable());
    }

    @Test
    public void addPartitionCarriesScalable() {
        assertTrue(parseFrame(Commands.newAddPartitionToTxn(1L, 2L, 0L, List.of("t"), true))
                .getAddPartitionToTxn().isScalable());
        assertFalse(parseFrame(Commands.newAddPartitionToTxn(1L, 2L, 0L, List.of("t")))
                .getAddPartitionToTxn().isScalable());
    }

    @Test
    public void addSubscriptionCarriesScalable() {
        assertTrue(parseFrame(Commands.newAddSubscriptionToTxn(1L, 2L, 0L, List.of(), true))
                .getAddSubscriptionToTxn().isScalable());
        assertFalse(parseFrame(Commands.newAddSubscriptionToTxn(1L, 2L, 0L, List.of()))
                .getAddSubscriptionToTxn().isScalable());
    }

    @Test
    public void defaultIsFalse() {
        // A command with no scalable field set (legacy/v4 client) must read false.
        assertEquals(parseFrame(Commands.newTxn(0L, 1L, 60_000L)).getNewTxn().isScalable(), false);
    }
}
