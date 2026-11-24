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
package org.apache.bookkeeper.mledger;

import io.opentelemetry.api.common.Attributes;
import lombok.Getter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.ManagedCursorOperationStatus;

@Getter
public class ManagedCursorAttributes {

    private final Attributes attributes;
    private final Attributes attributesOperationSucceed;
    private final Attributes attributesOperationFailure;

    public ManagedCursorAttributes(ManagedCursor cursor) {
        var mlName = cursor.getManagedLedger().getName();
        var topicName = TopicName.get(TopicName.fromPersistenceNamingEncoding(mlName));
        attributes = Attributes.of(
                OpenTelemetryAttributes.ML_CURSOR_NAME, cursor.getName(),
                OpenTelemetryAttributes.ML_LEDGER_NAME, mlName,
                OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace()
        );
        attributesOperationSucceed = Attributes.builder()
                .putAll(attributes)
                .putAll(ManagedCursorOperationStatus.SUCCESS.attributes)
                .build();
        attributesOperationFailure = Attributes.builder()
                .putAll(attributes)
                .putAll(ManagedCursorOperationStatus.FAILURE.attributes)
                .build();
    }
}
