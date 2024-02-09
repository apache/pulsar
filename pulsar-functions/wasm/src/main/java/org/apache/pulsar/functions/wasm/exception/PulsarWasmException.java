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

package org.apache.pulsar.functions.wasm.exception;

/**
 * pulsar WASM Exception.
 */
public class PulsarWasmException extends RuntimeException {

    private static final long serialVersionUID = 5939652370728356835L;

    /**
     * Instantiates a new pulsar WASM exception.
     *
     * @param e the e
     */
    public PulsarWasmException(final Throwable e) {
        super(e);
    }

    /**
     * Instantiates a new pulsar WASM exception.
     *
     * @param message the message
     */
    public PulsarWasmException(final String message) {
        super(message);
    }

    /**
     * Instantiates a new pulsar WASM exception.
     *
     * @param message   the message
     * @param throwable the throwable
     */
    public PulsarWasmException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
