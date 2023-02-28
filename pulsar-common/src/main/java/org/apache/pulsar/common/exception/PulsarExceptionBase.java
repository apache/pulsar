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
package org.apache.pulsar.common.exception;

import org.slf4j.helpers.MessageFormatter;

public abstract class PulsarExceptionBase extends Exception {

    protected static String format(String message, Object... os) {
        Throwable throwableCandidate = MessageFormatter.getThrowableCandidate(os);
        Object[] args = os;
        if (throwableCandidate != null) {
            args = MessageFormatter.trimmedCopy(os);
        }
        return MessageFormatter.arrayFormat(message, args, null).getMessage();
    }

    private static final long serialVersionUID = -2343132778619884518L;

    protected PulsarExceptionBase() {
        super();
    }

    protected PulsarExceptionBase(String message) {
        super(message);
    }

    protected PulsarExceptionBase(String message, Object... os) {
        this(format(message, os), MessageFormatter.getThrowableCandidate(os));
    }

    protected PulsarExceptionBase(Throwable cause) {
        super(cause);
    }

    protected PulsarExceptionBase(String message, Throwable cause) {
        super(message, cause);
    }

}