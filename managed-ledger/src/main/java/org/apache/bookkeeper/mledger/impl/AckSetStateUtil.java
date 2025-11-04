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

import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.bookkeeper.mledger.Position;

/**
 * Utility class to manage the ackSet state attached to a position.
 */
@UtilityClass
public class AckSetStateUtil {
    /**
     * Create a new position with the ackSet state.
     *
     * @param ledgerId ledger id
     * @param entryId entry id
     * @param ackSet ack set bitset information encoded as an array of longs
     * @return new position
     */
    public static Position createPositionWithAckSet(long ledgerId, long entryId, long[] ackSet) {
        return new AckSetPositionImpl(ledgerId, entryId, ackSet);
    }

    /**
     * Get the AckSetState instance from the position if it exists.
     * @param position position which possibly contains the AckSetState
     */
    public static Optional<AckSetState> maybeGetAckSetState(Position position) {
        return position.getExtension(AckSetState.class);
    }

    /**
     * Get the ackSet bitset information encoded as a long array from the position if it exists.
     * @param position position which possibly contains the AckSetState
     * @return the ackSet or null if the position does not have the AckSetState, or it's not set
     */
    public static long[] getAckSetArrayOrNull(Position position) {
        return maybeGetAckSetState(position).map(AckSetState::getAckSet).orElse(null);
    }

    /**
     * Get the AckSetState instance from the position.
     * @param position position which contains the AckSetState
     * @return AckSetState instance
     * @throws IllegalStateException if the position does not have AckSetState
     */
    public static AckSetState getAckSetState(Position position) {
        return maybeGetAckSetState(position)
                .orElseThrow(() ->
                        new IllegalStateException("Position does not have AckSetState. position=" + position));
    }

    /**
     * Check if position contains the ackSet information and it is set.
     * @param position position which possibly contains the AckSetState
     * @return true if the ackSet is set, false otherwise
     */
    public static boolean hasAckSet(Position position) {
        return maybeGetAckSetState(position).map(AckSetState::hasAckSet).orElse(false);
    }
}
