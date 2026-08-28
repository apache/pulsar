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

import java.util.concurrent.Semaphore;
import lombok.CustomLog;

/**
 * Mutex object that can be acquired from a thread and released from a different thread.
 *
 * <p/>This is meant to be acquired when calling an asynchronous method and released in its callback which is probably
 * executed in a different thread.
 */
@CustomLog
public class CallbackMutex {

    private Semaphore semaphore = new Semaphore(1, true);

    String owner;
    String position;

    public void lock() {
        semaphore.acquireUninterruptibly();

        log.debug(e -> {
            this.owner = Thread.currentThread().getName();
            this.position = Thread.currentThread().getStackTrace()[2].toString();
            e.attr("lockHash", this.hashCode())
                    .attr("position", position)
                    .log("Lock acquired");
        });
    }

    public boolean tryLock() {
        if (!semaphore.tryAcquire()) {
            return false;
        }

        log.debug(e -> {
            this.owner = Thread.currentThread().getName();
            this.position = Thread.currentThread().getStackTrace()[2].toString();
            e.attr("lockHash", this.hashCode())
                    .attr("position", position)
                    .log("Lock acquired");
        });
        return true;
    }

    public void unlock() {
        owner = null;
        position = null;
        log.debug(e -> e.attr("lockHash", this.hashCode())
                .attr("position", Thread.currentThread().getStackTrace()[2])
                .log("Lock released")
        );

        semaphore.release();
    }
}
