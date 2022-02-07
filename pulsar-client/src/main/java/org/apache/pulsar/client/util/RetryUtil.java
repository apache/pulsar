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
package org.apache.pulsar.client.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.client.impl.Backoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    public static <T> void retryAsynchronously(Supplier<CompletableFuture<T>> supplier, Backoff backoff,
                                               ScheduledExecutorService scheduledExecutorService,
                                               CompletableFuture<T> callback) {
        if (backoff.getMax() <= 0) {
            throw new IllegalArgumentException("Illegal max retry time");
        }
        if (backoff.getInitial() <= 0) {
            throw new IllegalArgumentException("Illegal initial time");
        }
        scheduledExecutorService.execute(() ->
                executeWithRetry(supplier, backoff, scheduledExecutorService, callback));
    }

    private static <T> void executeWithRetry(Supplier<CompletableFuture<T>> supplier, Backoff backoff,
                                             ScheduledExecutorService scheduledExecutorService,
                                             CompletableFuture<T> callback) {
        supplier.get().whenComplete((result, e) -> {
            if (e != null) {
                long next = backoff.next();
                boolean isMandatoryStop = backoff.isMandatoryStopMade();
                if (isMandatoryStop) {
                    callback.completeExceptionally(e);
                } else {
                    log.warn("Execution with retry fail, because of {}, will retry in {} ms", e.getMessage(), next);
                    scheduledExecutorService.schedule(() ->
                                    executeWithRetry(supplier, backoff, scheduledExecutorService, callback),
                            next, TimeUnit.MILLISECONDS);
                }
                return;
            }
            callback.complete(result);
        });
    }

}
