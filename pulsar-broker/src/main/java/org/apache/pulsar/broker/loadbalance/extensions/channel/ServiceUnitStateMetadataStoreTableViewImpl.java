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

package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.StorageType.MetadataStore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreTableView;
import org.apache.pulsar.metadata.tableview.impl.MetadataStoreTableViewImpl;

@Slf4j
public class ServiceUnitStateMetadataStoreTableViewImpl extends ServiceUnitStateTableViewBase {
    public static final String PATH_PREFIX = "/service_unit_state";
    private static final String VALID_PATH_REG_EX = "^\\/service_unit_state\\/.*\\/0x[0-9a-fA-F]{8}_0x[0-9a-fA-F]{8}$";
    private static final Pattern VALID_PATH_PATTERN;

    static {
        try {
            VALID_PATH_PATTERN = Pattern.compile(VALID_PATH_REG_EX);
        } catch (PatternSyntaxException error) {
            log.error("Invalid regular expression {}", VALID_PATH_REG_EX, error);
            throw new IllegalArgumentException(error);
        }
    }
    private ServiceUnitStateDataConflictResolver conflictResolver;
    private volatile MetadataStoreTableView<ServiceUnitStateData> tableview;

    public void start(PulsarService pulsar,
                      BiConsumer<String, ServiceUnitStateData> tailItemListener,
                      BiConsumer<String, ServiceUnitStateData> existingItemListener)
            throws MetadataStoreException {
        init(pulsar);
        conflictResolver = new ServiceUnitStateDataConflictResolver();
        conflictResolver.setStorageType(MetadataStore);
        tableview = new MetadataStoreTableViewImpl<>(ServiceUnitStateData.class,
                pulsar.getBrokerId(),
                pulsar.getLocalMetadataStore(),
                PATH_PREFIX,
                this::resolveConflict,
                this::validateServiceUnitPath,
                List.of(this::updateOwnedServiceUnits, tailItemListener),
                List.of(this::updateOwnedServiceUnits, existingItemListener),
                TimeUnit.SECONDS.toMillis(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds())
        );
        tableview.start();

    }

    protected boolean resolveConflict(ServiceUnitStateData prev, ServiceUnitStateData cur) {
        return !conflictResolver.shouldKeepLeft(prev, cur);
    }


    protected boolean validateServiceUnitPath(String path) {
        try {
            var matcher = VALID_PATH_PATTERN.matcher(path);
            return matcher.matches();
        } catch (Exception e) {
            return false;
        }
    }


    @Override
    public void close() throws IOException {
        if (tableview != null) {
            tableview = null;
            log.info("Successfully closed the channel tableview.");
        }
    }

    private boolean isValidState() {
        if (tableview == null) {
            return false;
        }
        return true;
    }

    @Override
    public ServiceUnitStateData get(String key) {
        if (!isValidState()) {
            throw new IllegalStateException(INVALID_STATE_ERROR_MSG);
        }
        return tableview.get(key);
    }

    @Override
    public CompletableFuture<Void> put(String key, @NonNull ServiceUnitStateData value) {
        if (!isValidState()) {
            return CompletableFuture.failedFuture(new IllegalStateException(INVALID_STATE_ERROR_MSG));
        }
        return tableview.put(key, value).exceptionally(e -> {
            if (e.getCause() instanceof MetadataStoreTableView.ConflictException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(e);
        });
    }

    @Override
    public void flush(long waitDurationInMillis) {
        // no-op
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        if (!isValidState()) {
            return CompletableFuture.failedFuture(new IllegalStateException(INVALID_STATE_ERROR_MSG));
        }
        return tableview.delete(key).exceptionally(e -> {
            if (e.getCause() instanceof MetadataStoreException.NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(e);
        });
    }


    @Override
    public Set<Map.Entry<String, ServiceUnitStateData>> entrySet() {
        if (!isValidState()) {
            throw new IllegalStateException(INVALID_STATE_ERROR_MSG);
        }
        return tableview.entrySet();
    }
}
