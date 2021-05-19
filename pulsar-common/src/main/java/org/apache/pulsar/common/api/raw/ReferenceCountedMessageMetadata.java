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
package org.apache.pulsar.common.api.raw;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * Class representing a reference-counted object that requires explicit deallocation.
 */
public class ReferenceCountedMessageMetadata extends AbstractReferenceCounted {

    private static final Recycler<ReferenceCountedMessageMetadata> RECYCLER = //
            new Recycler<ReferenceCountedMessageMetadata>() {
                @Override
                protected ReferenceCountedMessageMetadata newObject(Handle<ReferenceCountedMessageMetadata> handle) {
                    return new ReferenceCountedMessageMetadata(handle);
                }
            };

    private final MessageMetadata metadata = new MessageMetadata();
    private ByteBuf parsedBuf;
    private final Recycler.Handle<ReferenceCountedMessageMetadata> handle;

    private ReferenceCountedMessageMetadata(Recycler.Handle<ReferenceCountedMessageMetadata> handle) {
        this.handle = handle;
    }

    public static ReferenceCountedMessageMetadata get(ByteBuf parsedBuf) {
        ReferenceCountedMessageMetadata ref = RECYCLER.get();
        ref.parsedBuf = parsedBuf;
        ref.parsedBuf.retain();
        ref.setRefCnt(1);
        return ref;
    }

    public MessageMetadata getMetadata() {
        return metadata;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        if (parsedBuf != null) {
            parsedBuf.release();
        }
        metadata.clear();
        handle.recycle(this);
    }
}
