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
package org.apache.pulsar.broker.service.schema.exceptions;

import org.apache.pulsar.broker.service.BrokerServiceException;

/**
 * Schema related exceptions.
 */
public class SchemaException extends BrokerServiceException {

    private static final long serialVersionUID = -6587520779026691815L;
    private boolean recoverable;

    public SchemaException(boolean recoverable, String message) {
        super(message);
        this.recoverable = recoverable;
    }

    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(Throwable cause) {
        super(cause);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean isRecoverable() {
        return recoverable;
    }
}
