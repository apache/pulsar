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
package org.apache.pulsar.common.util.memory;

/**
 * Represents the result of an asynchronous semaphore acquisition.
 * Contains either a permit or information about why the acquisition failed.
 */
public interface AsyncSemaphorePermitResult {

    /**
     * Get the acquired permit if successful.
     *
     * @return the permit, or null if acquisition failed
     */
    AsyncSemaphorePermit getPermit();

    /**
     * Check if the permit acquisition was successful.
     *
     * @return true if acquisition was successful, false otherwise
     */
    boolean isSuccess();

    /**
     * Get the error message if acquisition failed.
     *
     * @return the error message, or null if acquisition was successful
     */
    String getErrorMessage();
}
