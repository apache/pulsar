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
package org.apache.pulsar.client.admin;

import org.apache.pulsar.client.api.MessageId;

/**
 * Status of offload process.
 */
public class OffloadProcessStatus extends LongRunningProcessStatus {

    public MessageId firstUnoffloadedMessage;

    public OffloadProcessStatus() {
        super(Status.NOT_RUN, "");
        firstUnoffloadedMessage = MessageId.earliest;
    }

    private OffloadProcessStatus(Status status, String lastError,
                                 MessageId firstUnoffloadedMessage) {
        this.status = status;
        this.lastError = lastError;
        this.firstUnoffloadedMessage = firstUnoffloadedMessage;
    }

    public static OffloadProcessStatus forStatus(Status status) {
        return new OffloadProcessStatus(status, "", MessageId.earliest);
    }

    public static OffloadProcessStatus forError(String lastError) {
        return new OffloadProcessStatus(Status.ERROR, lastError,
                MessageId.earliest);
    }

    public static OffloadProcessStatus forSuccess(MessageId messageId) {
        return new OffloadProcessStatus(Status.SUCCESS, "", messageId);
    }
}
