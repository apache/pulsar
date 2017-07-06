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
package io.netty.buffer;

public abstract class AbstractRecyclableDerivedByteBuf extends AbstractReferenceCountedByteBuf {
    protected ByteBuf origBuffer;

    protected AbstractRecyclableDerivedByteBuf(int maxCapacity) {
        super(maxCapacity);
    }

    protected void init(ByteBuf origBuffer) {
        // Retain 1 ref on the original buffer until the refCount of the derived buffer gets to 0
        this.origBuffer = origBuffer.retain();
        this.setRefCnt(1);
    }

    /**
     * Called when the ref count for the derived buffer gets to 0
     */
    protected void deallocate() {
        this.origBuffer.release();
        this.recycle();
    }

    abstract protected void recycle();
}
