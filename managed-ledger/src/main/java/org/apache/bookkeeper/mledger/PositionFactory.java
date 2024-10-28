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
package org.apache.bookkeeper.mledger;

import org.apache.bookkeeper.mledger.impl.ImmutablePositionImpl;

/**
 * Factory for creating {@link Position} instances.
 */
public final class PositionFactory {
    /**
     * Earliest position.
     */
    public static final Position EARLIEST = create(-1, -1);
    /**
     * Latest position.
     */
    public static final Position LATEST = create(Long.MAX_VALUE, Long.MAX_VALUE);

    private PositionFactory() {
    }

    /**
     * Create a new position.
     *
     * @param ledgerId ledger id
     * @param entryId entry id
     * @return new position
     */
    public static Position create(long ledgerId, long entryId) {
        return new ImmutablePositionImpl(ledgerId, entryId);
    }

    /**
     * Create a new position.
     *
     * @param other other position
     * @return new position
     */
    public static Position create(Position other) {
        return new ImmutablePositionImpl(other);
    }
}
