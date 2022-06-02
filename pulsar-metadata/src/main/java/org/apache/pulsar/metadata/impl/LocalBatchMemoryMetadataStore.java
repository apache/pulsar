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
package org.apache.pulsar.metadata.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.batching.AbstractBatchedMetadataStore;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.apache.pulsar.metadata.impl.batching.OpDelete;
import org.apache.pulsar.metadata.impl.batching.OpPut;

@Slf4j
public class LocalBatchMemoryMetadataStore extends AbstractBatchedMetadataStore implements MetadataStoreExtended {

    static final String BATCH_MEMORY_SCHEME_IDENTIFIER = "batch-memory:";

    private final LocalMemoryMetadataStore baseMetadataStore;

    public LocalBatchMemoryMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig, String type)
            throws MetadataStoreException {
        super(metadataStoreConfig);
        this.baseMetadataStore = new LocalMemoryMetadataStore(metadataURL, metadataStoreConfig, type);
        String name = metadataURL.substring(type.length());
        if ("local".equals(name)) {
            return;
        }
        // for notify invalid cache, so add self.
        LocalMemoryMetadataStore.STATIC_INSTANCE.compute(name, (key, value) -> {
            if (value == null) {
                value = new HashSet<>();
            }
            value.forEach(v -> {
                registerListener(v);
                v.registerListener(this);
            });
            value.add(this);
            return value;
        });
    }

    @Override
    public synchronized void batchOperation(List<MetadataOp> ops) {
        if (CollectionUtils.isEmpty(ops)) {
            return;
        }

        Map<String, LocalMemoryMetadataStore.Value> snapshot = baseMetadataStore.doSnapshot();

        List<CompletableFuture<?>> singleFutures = new ArrayList<>();

        for (int i = 0; i < ops.size(); i++) {
            MetadataOp op = ops.get(i);
            switch (op.getType()) {
                case GET: {
                    singleFutures.add(baseMetadataStore.get(op.asGet().getPath()));
                    break;
                }
                case PUT: {
                    OpPut p = op.asPut();
                    singleFutures.add(baseMetadataStore.put(p.getPath(), p.getData(), p.getOptExpectedVersion(),
                            p.getOptions()));
                    break;
                }
                case DELETE: {
                    OpDelete d = op.asDelete();
                    singleFutures.add(baseMetadataStore.delete(d.getPath(), d.getOptExpectedVersion()));
                    break;
                }
                case GET_CHILDREN: {
                    singleFutures.add(baseMetadataStore.getChildren(op.asGetChildren().getPath()));
                    break;
                }
            }
        }
        try {
            //sync to wait this batch operation, avoid data be modified
            FutureUtil.waitForAll(singleFutures).get();
            for (int i = 0; i < ops.size(); i++) {
                try {
                    completeOp(ops.get(i), singleFutures.get(i).get());
                } catch (InterruptedException | ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            baseMetadataStore.applySnapshot(snapshot);
            for (MetadataOp op : ops) {
                op.getFuture().completeExceptionally(e);
            }
        }
    }

    private void completeOp(MetadataOp op, Object result) {
        switch (op.getType()) {
            case PUT:
                op.asPut().getFuture().complete((Stat) result);
                break;
            case DELETE:
                op.asDelete().getFuture().complete((Void) result);
                break;
            case GET:
                op.asGet().getFuture().complete((Optional<GetResult>) result);
                break;
            case GET_CHILDREN:
                op.asGetChildren().getFuture().complete((List<String>) result);
                break;
            default:
                op.getFuture().completeExceptionally(new MetadataStoreException(
                        "Operation type not supported in multi: " + op.getType()));
        }
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return baseMetadataStore.existsFromStore(path);
    }
}
