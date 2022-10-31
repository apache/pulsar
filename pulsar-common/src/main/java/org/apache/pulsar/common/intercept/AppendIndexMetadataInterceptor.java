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
package org.apache.pulsar.common.intercept;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;

public class AppendIndexMetadataInterceptor implements BrokerEntryMetadataInterceptor{
    private final AtomicLong indexGenerator;

    public AppendIndexMetadataInterceptor() {
        this.indexGenerator = new AtomicLong(-1);
    }

    public void recoveryIndexGenerator(long index) {
        if (indexGenerator.get() < index) {
            indexGenerator.set(index);
        }
    }

    @Override
    public BrokerEntryMetadata intercept(BrokerEntryMetadata brokerMetadata) {
        // do nothing, just return brokerMetadata
        return brokerMetadata;
    }

    @Override
    public BrokerEntryMetadata interceptWithNumberOfMessages(
            BrokerEntryMetadata brokerMetadata,
            int numberOfMessages) {
        return brokerMetadata.setIndex(indexGenerator.addAndGet(numberOfMessages));
    }

    public long getIndex() {
        return indexGenerator.get();
    }
}
