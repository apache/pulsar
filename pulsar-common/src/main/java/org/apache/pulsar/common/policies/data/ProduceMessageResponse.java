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
package org.apache.pulsar.common.policies.data;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represent response of rest publish message attempt
 */
@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class ProduceMessageResponse {

    List<ProduceMessageResult> messagePublishResults;

    long schemaVersion;

    /**
     * Represent result of publishing a message through rest API.
     */
    @Setter
    @Getter
    public static class ProduceMessageResult {
        // partition message published to
        int partition;

        // message id in format of ledgerId:entryId
        String messageId;

        // error code, 0 means no error, 1 means retriable error, 2 means non=retriable error
        int errorCode;

        // error message is publish failed
        String error;
    }
}
