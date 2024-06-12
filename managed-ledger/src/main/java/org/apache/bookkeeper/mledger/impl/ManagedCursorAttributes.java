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
package org.apache.bookkeeper.mledger.impl;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import lombok.Data;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

@Data
public class ManagedCursorAttributes {

    public static final AttributeKey<String> PULSAR_MANAGED_CURSOR_NAME =
            AttributeKey.stringKey("pulsar.managed_cursor.name");

    public static final AttributeKey<String> PULSAR_MANAGED_CURSOR_OPERATION_STATUS =
            AttributeKey.stringKey("pulsar.managed_cursor.operation.status");

    @VisibleForTesting
    public enum OperationStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes =
                Attributes.of(PULSAR_MANAGED_CURSOR_OPERATION_STATUS, name().toLowerCase());
    }

    private final Attributes attributes;
    private final Attributes attributesOperationSucceed;
    private final Attributes attributesOperationFailure;

    public ManagedCursorAttributes(ManagedCursor cursor) {
        var mlName = cursor.getManagedLedger().getName();
        var topicName = TopicName.get(TopicName.fromPersistenceNamingEncoding(mlName));
        attributes = Attributes.of(
                PULSAR_MANAGED_CURSOR_NAME, cursor.getName(),
                ManagedLedgerAttributes.PULSAR_MANAGER_LEDGER_NAME, mlName,
                OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace()
        );
        attributesOperationSucceed =
                Attributes.builder().putAll(attributes).putAll(OperationStatus.SUCCESS.attributes).build();
        attributesOperationFailure =
                Attributes.builder().putAll(attributes).putAll(OperationStatus.FAILURE.attributes).build();
    }
}
