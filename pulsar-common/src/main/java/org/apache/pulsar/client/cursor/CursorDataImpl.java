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
package org.apache.pulsar.client.cursor;

import com.google.common.base.MoreObjects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.pulsar.client.api.CursorData;
import org.apache.pulsar.common.api.proto.CursorPosition;
import org.apache.pulsar.common.api.proto.EntryPosition;
import org.apache.pulsar.common.util.ToStringUtil;

/**
 * A CursorData implementation.
 */
@AllArgsConstructor
public class CursorDataImpl implements CursorData {

    /**
     * The latest managedLedger info version of the topic, which this cursor belongs to.
     */
    @Getter
    private long version;

    /**
     * The lastConfirmedEntry of the topic, which this cursor belongs to.
     */
    @Getter
    private EntryPosition lastConfirmedEntry;

    /**
     * The cursor persistent info of this cursor.
     */
    @Getter
    private final CursorPosition position;

    public CursorDataImpl(CursorPosition position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("version", version)
                .add("lastConfirmedEntry", lastConfirmedEntry.getLedgerId() + ":" + lastConfirmedEntry.getEntryId())
                .add("position", ToStringUtil.pbObjectToString(position))
                .toString();
    }
}
