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
package org.apache.pulsar.packages.management.core.exceptions;

/**
 * Packages management related exceptions.
 */
public class PackagesManagementException extends Exception {
    /**
     * Constructs an {@code PackagesManagementException} with the specified cause.
     *
     * @param throwable
     *          The cause
     */
    public PackagesManagementException(Throwable throwable) {
        super(throwable);
    }

    /**
     * Constructs an {@code PackagesManagementException} with the specified detail message.
     *
     * @param message
     *          The detail message
     */
    public PackagesManagementException(String message) {
        super(message);
    }

    /**
     * Constructs an {@code PackagesManagementException} with the specified detail message and the cause.
     *
     * @param message
     *          The detail message
     * @param throwable
     *          The cause
     */
    public PackagesManagementException(String message, Throwable throwable) {
        super(message, throwable);
    }


    public static class NotFoundException extends PackagesManagementException {
        /**
         * Constructs an {@code NotFoundException} with the specified cause.
         *
         * @param throwable
         *          The cause
         */
        public NotFoundException(Throwable throwable) {
            super(throwable);
        }

        /**
         * Constructs an {@code NotFoundException} with the specified detail message.
         *
         * @param message
         *          The detail message
         */
        public NotFoundException(String message) {
            super(message);
        }

        /**
         * Constructs an {@code NotFoundException} with the specified detail message and the cause.
         *
         * @param message
         *          The detail message
         * @param throwable
         *          The cause
         */
        public NotFoundException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }

    public static class MetadataFormatException extends PackagesManagementException {
        /**
         * Constructs an {@code MetadataFormatException} with the specified detail message.
         *
         * @param message
         *          The detail message
         */
        public MetadataFormatException(String message) {
            super(message);
        }

        /**
         * Constructs an {@code MetadataFormatException} with the specified detail message and the cause.
         *
         * @param message
         *          The detail message
         * @param throwable
         *          The cause
         */
        public MetadataFormatException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }
}

