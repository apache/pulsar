/*
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
package org.apache.pulsar.transaction.coordinator;

import io.opentelemetry.api.common.Attributes;
import lombok.Getter;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

@Getter
public class TransactionMetadataStoreAttributes {

    private final Attributes commonAttributes;
    private final Attributes txnAbortedAttributes;
    private final Attributes txnActiveAttributes;
    private final Attributes txnCommittedAttributes;
    private final Attributes txnCreatedAttributes;
    private final Attributes txnTimeoutAttributes;

    public TransactionMetadataStoreAttributes(TransactionMetadataStore store) {
        this.commonAttributes = Attributes.of(
                OpenTelemetryAttributes.PULSAR_TRANSACTION_COORDINATOR_ID, store.getTransactionCoordinatorID().getId());
        this.txnAbortedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes)
                .build();
        this.txnActiveAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ACTIVE.attributes)
                .build();
        this.txnCommittedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .build();
        this.txnCreatedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.CREATED.attributes)
                .build();
        this.txnTimeoutAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.TIMEOUT.attributes)
                .build();
    }
}
