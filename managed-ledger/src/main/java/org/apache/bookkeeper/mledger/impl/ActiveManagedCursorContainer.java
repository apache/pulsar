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

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

public interface ActiveManagedCursorContainer extends Iterable<ManagedCursor> {
    void add(ManagedCursor cursor, Position position);

    ManagedCursor get(String name);

    boolean removeCursor(String name);

    Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition);

    Position getSlowestCursorPosition();

    boolean isEmpty();

    int size();

    default int getNumberOfCursorsAtSamePositionOrBefore(ManagedCursor cursor) {
        throw new UnsupportedOperationException("This method is not supported by this implementation");
    }
}