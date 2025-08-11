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

import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

public interface ManagedCursorContainer extends Iterable<ManagedCursor> {
    void add(ManagedCursor cursor, Position position);

    ManagedCursor get(String name);

    boolean removeCursor(String name);

    Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition);

    Position getSlowestCursorPosition();

    ManagedCursor getSlowestCursor();

    CursorInfo getCursorWithOldestPosition();

    boolean isEmpty();

    int size();

    boolean hasDurableCursors();

    @Value
    class CursorInfo {
        ManagedCursor cursor;
        Position position;

        /**
         * Cursor info's version.
         * <p>
         * Use {@link  DataVersion#compareVersions(long, long)} to compare between two versions,
         * since it rolls over to 0 once reaching Long.MAX_VALUE
         */
        long version;
    }

    /**
     * Utility class to manage a data version, which rolls over to 0 when reaching Long.MAX_VALUE.
     */
    @UtilityClass
    class DataVersion {

        /**
         * Compares two data versions, which either rolls overs to 0 when reaching Long.MAX_VALUE.
         * <p>
         * Use {@link DataVersion#getNextVersion(long)} to increment the versions. The assumptions
         * are that metric versions are compared with close time proximity one to another, hence,
         * they are expected not close to each other in terms of distance, hence we don't
         * expect the distance ever to exceed Long.MAX_VALUE / 2, otherwise we wouldn't be able
         * to know which one is a later version in case the furthest rolls over to beyond 0. We
         * assume the shortest distance between them dictates that.
         * <p>
         *
         * @param v1 First version to compare
         * @param v2 Second version to compare
         * @return the value {@code 0} if {@code v1 == v2};
         * a value less than {@code 0} if {@code v1 < v2}; and
         * a value greater than {@code 0} if {@code v1 > v2}
         */
        public static int compareVersions(long v1, long v2) {
            if (v1 == v2) {
                return 0;
            }

            // 0-------v1--------v2--------MAX_LONG
            if (v2 > v1) {
                long distance = v2 - v1;
                long wrapAroundDistance = (Long.MAX_VALUE - v2) + v1;
                if (distance < wrapAroundDistance) {
                    return -1;
                } else {
                    return 1;
                }

                // 0-------v2--------v1--------MAX_LONG
            } else {
                long distance = v1 - v2;
                long wrapAroundDistance = (Long.MAX_VALUE - v1) + v2;
                if (distance < wrapAroundDistance) {
                    return 1; // v1 is bigger
                } else {
                    return -1; // v2 is bigger
                }
            }
        }

        public static long getNextVersion(long existingVersion) {
            if (existingVersion == Long.MAX_VALUE) {
                return 0;
            } else {
                return existingVersion + 1;
            }
        }
    }
}
