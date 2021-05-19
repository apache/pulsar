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

import org.apache.pulsar.client.admin.utils.DefaultImplementation;
import org.apache.pulsar.client.api.MessageId;

/**
 * interface class of Status of offload process.
 */
public interface OffloadProcessStatus {

    MessageId getFirstUnoffloadedMessage();
    String getLastError();
    LongRunningProcessStatus.Status getStatus();

    static OffloadProcessStatus forStatus(LongRunningProcessStatus.Status status) {
        return DefaultImplementation.newOffloadProcessStatus(status, "", MessageId.earliest);
    }

    static OffloadProcessStatus forError(String lastError) {
        return DefaultImplementation.newOffloadProcessStatus(LongRunningProcessStatus.Status.ERROR, lastError,
                MessageId.earliest);
    }

    static OffloadProcessStatus forSuccess(MessageId messageId) {
        return DefaultImplementation.newOffloadProcessStatus(LongRunningProcessStatus.Status.SUCCESS, "",
                messageId);
    }

}
