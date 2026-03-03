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
package org.apache.bookkeeper.mledger.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ManagedLedgerTestUtil {

    public static <T> T retry(ThrowingSupplier<T> supplier) {
        return retry(10, supplier);
    }

    public static <T> T retry(int retryCount, ThrowingSupplier<T> supplier) {
        for (int i = 0; i < retryCount; i++) {
            if (i > 0) {
                try {
                    log.info("Retrying after 100ms {}/{}", i, retryCount);
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                return supplier.get();
            } catch (Exception e) {
                log.warn("Failed to execute supplier: {}", supplier, e);
            }
        }
        throw new RuntimeException("Failed to execute supplier after " + retryCount  + " retries");
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

}
