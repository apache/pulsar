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

import org.apache.bookkeeper.mledger.Position;

final class MutablePositionImpl implements Position {

    private volatile long ledgerId;
    private volatile long entryId;

    MutablePositionImpl(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    MutablePositionImpl() {
        this.ledgerId = -1;
        this.entryId = -1;
    }

    /**
     * Change the ledgerId and entryId.
     *
     * @param ledgerId
     * @param entryId
     */
    public void changePositionTo(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    /**
     * String representation of virtual cursor - LedgerId:EntryId.
     */
    @Override
    public String toString() {
        return ledgerId + ":" + entryId;
    }

    @Override
    public int hashCode() {
        return hashCodeForPosition();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Position && compareTo((Position) obj) == 0;
    }

}
