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
package org.apache.pulsar.common.util;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Runnables {
    private static final Logger LOGGER = LoggerFactory.getLogger(Runnables.class);

    private Runnables() {}

    /**
     * Wraps a Runnable so that throwables are caught and logged when a Runnable is run.
     *
     * The main usecase for this method is to be used in {@link java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}
     * calls to ensure that the scheduled task doesn't get cancelled as a result of an uncaught exception.
     *
     * @param runnable The runnable to wrap
     * @return a wrapped Runnable
     */
    public static Runnable catchingAndLoggingThrowables(Runnable runnable) {
        return new CatchingAndLoggingRunnable(runnable);
    }

    private static final class CatchingAndLoggingRunnable implements Runnable {
        private final Runnable runnable;

        private CatchingAndLoggingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                LOGGER.error("Unexpected throwable caught", t);
            }
        }
    }
}