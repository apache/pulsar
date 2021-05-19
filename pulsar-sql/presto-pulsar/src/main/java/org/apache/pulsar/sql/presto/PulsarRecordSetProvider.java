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
package org.apache.pulsar.sql.presto;

import static java.util.Objects.requireNonNull;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;
import java.util.List;
import javax.inject.Inject;

/**
 * Implementation of the provider for record sets.
 */
public class PulsarRecordSetProvider implements ConnectorRecordSetProvider {

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final PulsarDispatchingRowDecoderFactory decoderFactory;

    @Inject
    public PulsarRecordSetProvider(PulsarConnectorConfig pulsarConnectorConfig,
                                   PulsarDispatchingRowDecoderFactory decoderFactory) {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                  ConnectorSplit split, List<? extends ColumnHandle> columns) {

        requireNonNull(split, "Connector split is null");
        PulsarSplit pulsarSplit = (PulsarSplit) split;

        ImmutableList.Builder<PulsarColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((PulsarColumnHandle) handle);
        }

        return new PulsarRecordSet(pulsarSplit, handles.build(), this.pulsarConnectorConfig, decoderFactory);
    }
}
