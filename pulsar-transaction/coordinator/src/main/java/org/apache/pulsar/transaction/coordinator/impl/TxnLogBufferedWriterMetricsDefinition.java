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
package org.apache.pulsar.transaction.coordinator.impl;

import lombok.Getter;
import lombok.Setter;

public class TxnLogBufferedWriterMetricsDefinition {

    public static final TxnLogBufferedWriterMetricsDefinition DISABLED =
            new TxnLogBufferedWriterMetricsDefinition(false, "", new String[]{}, new String[]{});

    public static final String COMPONENT_TRANSACTION_COORDINATOR = "pulsar_txn_tc";

    public static final String COMPONENT_TRANSACTION_PENDING_ACK_STORE = "pulsar_txn_pending_ack";

    @Getter
    @Setter
    private final boolean enabled;

    @Getter
    @Setter
    private final String component;

    final String[] labelNames;

    final String[] labelValues;

    public TxnLogBufferedWriterMetricsDefinition copy(){
        return new TxnLogBufferedWriterMetricsDefinition(enabled, component, labelNames, labelValues);
    }

    public TxnLogBufferedWriterMetricsDefinition(boolean enabled, String component, String[] labelNames,
                                                 String[] labelValues){
        this.enabled = enabled;
        this.component = component;
        this.labelNames = labelNames.clone();
        this.labelValues = labelValues.clone();
    }
}
