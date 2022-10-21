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
package org.apache.pulsar.common.protocol.topic;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeleteLedgerPayload {

    private long ledgerId;

    private String topicName;

    private String ledgerType;

    private String ledgerComponent;

    private OffloadContext offloadContext;

    private Map<String, String> properties;

    public DeleteLedgerPayload(Long ledgerId, String topicName, String ledgerType, String ledgerComponent) {
        this.ledgerId = ledgerId;
        this.topicName = topicName;
        this.ledgerType = ledgerType;
        this.ledgerComponent = ledgerComponent;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OffloadContext {
        private long lsb;
        private long msb;
        private String driverName;
        private Map<String, String> metadata;
    }

}
