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
package org.apache.pulsar.common.util;

import java.util.concurrent.ExecutionException;

/**
 * Util class to place some special handling of exceptions.
 */
public class ExceptionHandler {

    /**
     * This utility class should not be instantiated.
     */
    private ExceptionHandler() {
    }

    /**
     * If the throwable is InterruptedException, reset the thread interrupted flag.
     * We can use it in catching block when we need catch the InterruptedException
     * and reset the thread interrupted flag no matter whether the throwable being caught is InterruptedException.
     *
     * @param throwable the throwable being caught
     */
    public static void handleInterruptedException(Throwable throwable) {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else if (throwable instanceof ExecutionException) {
            handleInterruptedException(throwable.getCause());
        }
    }
}
