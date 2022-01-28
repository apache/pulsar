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
package org.apache.pulsar.broker.service;

import io.netty.util.Recycler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.MessageMetadata;

public class EntryWrapper {
    private Entry entry = null;
    private MessageMetadata metadata = new MessageMetadata();
    private boolean hasMetadata = false;

    public static EntryWrapper get(Entry entry, MessageMetadata metadata) {
        EntryWrapper entryWrapper = RECYCLER.get();
        entryWrapper.entry = entry;
        if (metadata != null) {
            entryWrapper.hasMetadata = true;
            entryWrapper.metadata.copyFrom(metadata);
        }
        entryWrapper.metadata.copyFrom(metadata);
        return entryWrapper;
    }

    private EntryWrapper(Recycler.Handle<EntryWrapper> handle) {
        this.handle = handle;
    }

    public Entry getEntry() {
        return entry;
    }

    public MessageMetadata getMetadata() {
        return hasMetadata ? metadata : null;
    }

    private final Recycler.Handle<EntryWrapper> handle;
    private static final Recycler<EntryWrapper> RECYCLER = new Recycler<EntryWrapper>() {
        @Override
        protected EntryWrapper newObject(Handle<EntryWrapper> handle) {
            return new EntryWrapper(handle);
        }
    };

    public void recycle() {
        entry = null;
        hasMetadata = false;
        metadata.clear();
        handle.recycle(this);
    }
}