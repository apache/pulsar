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
package org.apache.pulsar.common.schema;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Exception thrown when schema cache operations fail.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SchemaCacheException extends PulsarClientException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs an exception with the specified error message.
     *
     * @param message the error message
     */
    public SchemaCacheException(String message) {
        super(message);
    }

    /**
     * Constructs an exception with the specified error message and cause.
     *
     * @param message the error message
     * @param cause the cause of the error
     */
    public SchemaCacheException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an exception with the specified cause.
     *
     * @param cause the cause of the error
     */
    public SchemaCacheException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates an exception indicating schema creation failure.
     *
     * @param className the name of the class for which schema creation failed
     * @param cause the cause of the failure
     * @return a new SchemaCacheException instance
     */
    public static SchemaCacheException schemaCreationFailed(String className, Throwable cause) {
        return new SchemaCacheException(
            String.format("Failed to create schema for class: %s", className), cause);
    }

    /**
     * Creates an exception indicating schema cloning failure.
     *
     * @param className the name of the class for which schema cloning failed
     * @param cause the cause of the failure
     * @return a new SchemaCacheException instance
     */
    public static SchemaCacheException schemaCloneFailed(String className, Throwable cause) {
        return new SchemaCacheException(
            String.format("Failed to clone schema for class: %s", className), cause);
    }

    /**
     * Creates an exception indicating cache initialization failure.
     *
     * @param cause the cause of the failure
     * @return a new SchemaCacheException instance
     */
    public static SchemaCacheException initializationFailed(Throwable cause) {
        return new SchemaCacheException("Failed to initialize schema cache", cause);
    }

    /**
     * Creates an exception indicating cache configuration error.
     *
     * @param message the error message describing the configuration problem
     * @return a new SchemaCacheException instance
     */
    public static SchemaCacheException invalidConfiguration(String message) {
        return new SchemaCacheException("Invalid schema cache configuration: " + message);
    }

    /**
     * Creates an exception indicating cache access error.
     *
     * @param operation the operation that failed
     * @param cause the cause of the failure
     * @return a new SchemaCacheException instance
     */
    public static SchemaCacheException operationFailed(String operation, Throwable cause) {
        return new SchemaCacheException(
            String.format("Schema cache operation failed: %s", operation), cause);
    }

    @Override
    public String toString() {
        if (getCause() != null) {
            return String.format("SchemaCacheException: %s, caused by: %s", 
                getMessage(), getCause().toString());
        }
        return String.format("SchemaCacheException: %s", getMessage());
    }
}