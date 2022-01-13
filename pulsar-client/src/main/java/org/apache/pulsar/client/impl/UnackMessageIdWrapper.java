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

import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;

@Getter
public class UnackMessageIdWrapper {

    private MessageId messageId;
    private int redeliveryCount = 0;

    private UnackMessageIdWrapper(MessageId messageId) {
        this(messageId, 0);
    }

    @Override
    public String toString() {
        return "UnackMessageIdWrapper [messageId=" + messageId + ", redeliveryCount=" + redeliveryCount + "]";
    }

    private UnackMessageIdWrapper(MessageId messageId, int redeliveryCount) {
        this.messageId = messageId;
        this.redeliveryCount = redeliveryCount;
    }

    public static UnackMessageIdWrapper valueOf(MessageId messageId) {
        return new UnackMessageIdWrapper(messageId);
    }

    public static UnackMessageIdWrapper valueOf(MessageId messageId, int redeliveryCount) {
        return new UnackMessageIdWrapper(messageId, redeliveryCount);
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

}
