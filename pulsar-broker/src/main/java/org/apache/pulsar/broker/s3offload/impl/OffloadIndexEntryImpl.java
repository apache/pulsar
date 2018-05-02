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
package org.apache.pulsar.broker.s3offload.impl;

import org.apache.pulsar.broker.s3offload.OffloadIndexEntry;

/**
 *
 * The Index Entry in OffloadIndexBlock.
 *
 */
public class OffloadIndexEntryImpl implements OffloadIndexEntry {
    public static OffloadIndexEntryImpl of(long entryId, int partId, long offset) {
        return new OffloadIndexEntryImpl(entryId, partId, offset);
    }

    private final long entryId;

    private final int partId;

    private final long offset;

    @Override
    public long getEntryId() {
        return entryId;
    }
    @Override
    public int getPartId() {
        return partId;
    }
    @Override
    public long getOffset() {
        return offset;
    }

    public OffloadIndexEntryImpl(long entryId, int partId, long offset) {
        this.entryId = entryId;
        this.partId = partId;
        this.offset = offset;
    }
}

