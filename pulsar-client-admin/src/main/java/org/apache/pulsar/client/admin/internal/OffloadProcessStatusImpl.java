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
package org.apache.pulsar.client.admin.internal;

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * Status of offload process.
 */
public class OffloadProcessStatusImpl extends LongRunningProcessStatus implements OffloadProcessStatus {
    public MessageIdImpl firstUnoffloadedMessage;

    public OffloadProcessStatusImpl() {
        status = Status.NOT_RUN;
        lastError = "";
        firstUnoffloadedMessage = (MessageIdImpl) MessageId.earliest;
    }

    public OffloadProcessStatusImpl(Status status, String lastError,
                                 MessageId firstUnoffloadedMessage) {
        this.status = status;
        this.lastError = lastError;
        this.firstUnoffloadedMessage = (MessageIdImpl) firstUnoffloadedMessage;
    }

    @Override
    public MessageId getFirstUnoffloadedMessage() {
        return firstUnoffloadedMessage;
    }

    @Override
    public String getLastError() {
        return lastError;
    }

    @Override
    public Status getStatus() {
        return status;
    }
}
