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
package org.apache.bookkeeper.mledger.util;

import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mutex object that can be acquired from a thread and released from a different thread.
 *
 * <p/>This is meant to be acquired when calling an asynchronous method and released in its callback which is probably
 * executed in a different thread.
 */
public class CallbackMutex {

    private Semaphore semaphore = new Semaphore(1, true);

    String owner;
    String position;

    public void lock() {
        semaphore.acquireUninterruptibly();

        if (log.isDebugEnabled()) {
            owner = Thread.currentThread().getName();
            position = Thread.currentThread().getStackTrace()[2].toString();
            log.debug("<<< Lock {} acquired at {}", this.hashCode(), position);
        }
    }

    public boolean tryLock() {
        if (!semaphore.tryAcquire()) {
            return false;
        }

        if (log.isDebugEnabled()) {
            owner = Thread.currentThread().getName();
            position = Thread.currentThread().getStackTrace()[2].toString();
            log.debug("<<< Lock {} acquired at {}", this.hashCode(), position);
        }
        return true;
    }

    public void unlock() {
        if (log.isDebugEnabled()) {
            owner = null;
            position = null;
            log.debug(">>> Lock {} released at {}", this.hashCode(),
                    Thread.currentThread().getStackTrace()[2]);
        }

        semaphore.release();
    }

    private static final Logger log = LoggerFactory.getLogger(CallbackMutex.class);
}
