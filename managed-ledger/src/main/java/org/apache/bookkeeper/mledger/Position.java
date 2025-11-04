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

import java.util.Optional;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * A Position is a pointer to a specific entry into the managed ledger.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface Position extends Comparable<Position> {
    /**
     * Get the ledger id of the entry pointed by this position.
     *
     * @return the ledger id
     */
    long getLedgerId();

    /**
     * Get the entry id of the entry pointed by this position.
     *
     * @return the entry id
     */
    long getEntryId();

    /**
     * Compare this position with another position.
     * The comparison is first based on the ledger id, and then on the entry id.
     * This is implements the Comparable interface.
     * @param that the other position to be compared.
     * @return -1 if this position is less than the other, 0 if they are equal, 1 if this position is greater than
     * the other.
     */
    default int compareTo(Position that) {
        if (getLedgerId() != that.getLedgerId()) {
            return Long.compare(getLedgerId(), that.getLedgerId());
        }

        return Long.compare(getEntryId(), that.getEntryId());
    }

    /**
     * Compare this position with another position based on the ledger id and entry id.
     * @param ledgerId the ledger id to compare
     * @param entryId the entry id to compare
     * @return -1 if this position is less than the other, 0 if they are equal, 1 if this position is greater than
     * the other.
     */
    default int compareTo(long ledgerId, long entryId) {
        if (getLedgerId() != ledgerId) {
            return Long.compare(getLedgerId(), ledgerId);
        }

        return Long.compare(getEntryId(), entryId);
    }

    /**
     * Calculate the hash code for the position based on ledgerId and entryId.
     * This is used in Position implementations to implement the hashCode method.
     * @return hash code
     */
    default int hashCodeForPosition() {
        int result = Long.hashCode(getLedgerId());
        result = 31 * result + Long.hashCode(getEntryId());
        return result;
    }

    /**
     * Get the position of the entry next to this one. The returned position might point to a non-existing, or not-yet
     * existing entry
     *
     * @return the position of the next logical entry
     */
    default Position getNext() {
        if (getEntryId() < 0) {
            return PositionFactory.create(getLedgerId(), 0);
        } else {
            return PositionFactory.create(getLedgerId(), getEntryId() + 1);
        }
    }

    /**
     * Position after moving entryNum messages,
     * if entryNum < 1, then return the current position.
     * */
    default Position getPositionAfterEntries(int entryNum) {
        if (entryNum < 1) {
            return this;
        }
        if (getEntryId() < 0) {
            return PositionFactory.create(getLedgerId(), entryNum - 1);
        } else {
            return PositionFactory.create(getLedgerId(), getEntryId() + entryNum);
        }
    }

    /**
     * Check if the position implementation has an extension of the given class or interface.
     *
     * @param extensionClass the class of the extension
     * @return true if the position has an extension of the given class, false otherwise
     */
    default boolean hasExtension(Class<?> extensionClass) {
        return getExtension(extensionClass).isPresent();
    }

    /**
     * Get the extension instance of the given class or interface that is attached to this position.
     * If the position does not have an extension of the given class, an empty optional is returned.
     * @param extensionClass the class of the extension
     */
    default <T> Optional<T> getExtension(Class<T> extensionClass) {
        return Optional.empty();
    }
}
