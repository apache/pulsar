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
package org.apache.pulsar.compaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compactor for Pulsar topics.
 */
public abstract class Compactor {
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);
    public static final String COMPACTION_SUBSCRIPTION = "__compaction";
    public static final String COMPACTED_TOPIC_LEDGER_PROPERTY = "CompactedTopicLedger";
    static final BookKeeper.DigestType COMPACTED_TOPIC_LEDGER_DIGEST_TYPE = BookKeeper.DigestType.CRC32;
    static final byte[] COMPACTED_TOPIC_LEDGER_PASSWORD = "".getBytes(UTF_8);

    protected final ServiceConfiguration conf;
    protected final ScheduledExecutorService scheduler;
    private final PulsarClient pulsar;
    private final BookKeeper bk;
    protected final CompactorMXBeanImpl mxBean;

    public Compactor(ServiceConfiguration conf,
                     PulsarClient pulsar,
                     BookKeeper bk,
                     ScheduledExecutorService scheduler) {
        this.conf = conf;
        this.scheduler = scheduler;
        this.pulsar = pulsar;
        this.bk = bk;
        this.mxBean = new CompactorMXBeanImpl();
    }

    public CompletableFuture<Long> compact(String topic) {
        return RawReader.create(pulsar, topic, COMPACTION_SUBSCRIPTION).thenComposeAsync(
                this::compactAndCloseReader, scheduler);
    }

    private CompletableFuture<Long> compactAndCloseReader(RawReader reader) {
        CompletableFuture<Long> promise = new CompletableFuture<>();
        mxBean.addCompactionStartOp(reader.getTopic());
        doCompaction(reader, bk).whenComplete(
                (ledgerId, exception) -> {
                    reader.closeAsync().whenComplete((v, exception2) -> {
                        if (exception2 != null) {
                            log.warn("Error closing reader handle {}, ignoring", reader, exception2);
                        }
                        if (exception != null) {
                            // complete with original exception
                            mxBean.addCompactionEndOp(reader.getTopic(), false);
                            promise.completeExceptionally(exception);
                        } else {
                            mxBean.addCompactionEndOp(reader.getTopic(), true);
                            promise.complete(ledgerId);
                        }
                    });
                });
        return promise;
    }

    protected abstract CompletableFuture<Long> doCompaction(RawReader reader, BookKeeper bk);

    public CompactorMXBean getStats() {
        return this.mxBean;
    }
}

