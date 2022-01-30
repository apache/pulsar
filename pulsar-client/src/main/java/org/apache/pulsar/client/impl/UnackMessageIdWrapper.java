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
package org.apache.pulsar.client.impl;

import io.netty.util.Recycler;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;

@Getter
public class UnackMessageIdWrapper {

    private static final Recycler<UnackMessageIdWrapper> RECYCLER = new Recycler<UnackMessageIdWrapper>() {
        @Override
        protected UnackMessageIdWrapper newObject(Handle<UnackMessageIdWrapper> handle) {
            return new UnackMessageIdWrapper(handle);
        }
    };

    private final Recycler.Handle<UnackMessageIdWrapper> recyclerHandle;
    private MessageId messageId;
    private int redeliveryCount = 0;

    private UnackMessageIdWrapper(Recycler.Handle<UnackMessageIdWrapper> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static UnackMessageIdWrapper create(MessageId messageId, int redeliveryCount) {
        UnackMessageIdWrapper unackMessageIdWrapper = RECYCLER.get();
        unackMessageIdWrapper.messageId = messageId;
        unackMessageIdWrapper.redeliveryCount = redeliveryCount;
        return unackMessageIdWrapper;
    }


    public static UnackMessageIdWrapper valueOf(MessageId messageId) {
        return create(messageId, 0);
    }

    public static UnackMessageIdWrapper valueOf(MessageId messageId, int redeliveryCount) {
        return create(messageId, redeliveryCount);
    }

    public void recycle() {
        messageId = null;
        redeliveryCount = 0;
        recyclerHandle.recycle(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj instanceof UnackMessageIdWrapper) {
            UnackMessageIdWrapper other = (UnackMessageIdWrapper) obj;
            if (this.messageId.equals(other.messageId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (messageId == null ? 0 : messageId.hashCode());
    }

    @Override
    public String toString() {
        return "UnackMessageIdWrapper [messageId=" + messageId + ", redeliveryCount=" + redeliveryCount + "]";
    }

}
