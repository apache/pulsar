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
package org.apache.pulsar.common.intercept;

import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.concurrent.atomic.AtomicLong;

public class AppendOffsetMetadataInterceptor implements BrokerEntryMetadataInterceptor{
    private final AtomicLong offsetGenerator;

    public AppendOffsetMetadataInterceptor() {
        this.offsetGenerator = new AtomicLong(-1);
    }

    public void recoveryOffsetGenerator(long offset) {
        if (offsetGenerator.get() < offset) {
            offsetGenerator.set(offset);
        }
    }

    @Override
    public PulsarApi.BrokerEntryMetadata.Builder intercept(PulsarApi.BrokerEntryMetadata.Builder brokerMetadata) {
        // do nothing, just return brokerMetadata
        return brokerMetadata;
    }

    @Override
    public PulsarApi.BrokerEntryMetadata.Builder interceptWithBatchSize(
            PulsarApi.BrokerEntryMetadata.Builder brokerMetadata,
            int batchSize) {
        return brokerMetadata.setOffset(offsetGenerator.addAndGet(batchSize));
    }

    public long getOffset() {
        return offsetGenerator.get();
    }
}
