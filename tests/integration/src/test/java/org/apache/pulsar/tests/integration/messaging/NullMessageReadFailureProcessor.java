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
package org.apache.pulsar.tests.integration.messaging;

import io.netty.buffer.ByteBuf;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;

/** Source broker fixture for exercising the cursor failure callback with a null exception message. */
public final class NullMessageReadFailureProcessor implements ManagedLedgerPayloadProcessor,
        ManagedLedgerPayloadProcessor.Processor {
    static final String MARKER = "pip-502-fail-read";
    static final String SWITCH = "/tmp/pip-502-fail-read";

    @Override
    public Processor outputProcessor() {
        return this;
    }

    @Override
    public ByteBuf process(Object context, ByteBuf entry) {
        var metadata = Commands.peekMessageMetadata(entry, MARKER, -1);
        if (metadata != null && metadata.getPropertiesList().stream().anyMatch(p -> MARKER.equals(p.getKey()))
                && Files.exists(Path.of(SWITCH))) {
            // The ledger hands the processor a retained duplicate. No handle is returned on failure.
            entry.release();
            throw new CompletionException(new ManagedLedgerException((String) null));
        }
        return entry;
    }

    @Override
    public void release(ByteBuf entry) {
        // Successful reads pass the original buffer through.
    }
}
