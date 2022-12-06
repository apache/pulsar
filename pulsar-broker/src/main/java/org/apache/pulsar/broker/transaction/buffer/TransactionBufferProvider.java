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
package org.apache.pulsar.broker.transaction.buffer;

import com.google.common.annotations.Beta;
import java.io.IOException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.util.Reflections;

/**
 * A provider that provides {@link TransactionBuffer}.
 */
@Beta
public interface TransactionBufferProvider {

    /**
     * Construct a provider from the provided class.
     *
     * @param providerClassName the provider class name.
     * @return an instance of transaction buffer provider.
     */
    static TransactionBufferProvider newProvider(String providerClassName) throws IOException {
        try {
            TransactionBufferProvider transactionBufferProvider = Reflections.createInstance(providerClassName,
                    TransactionBufferProvider.class, Thread.currentThread().getContextClassLoader());
            return transactionBufferProvider;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Open the persistent transaction buffer.
     *
     * @param originTopic
     * @return
     */
    TransactionBuffer newTransactionBuffer(Topic originTopic);
}
