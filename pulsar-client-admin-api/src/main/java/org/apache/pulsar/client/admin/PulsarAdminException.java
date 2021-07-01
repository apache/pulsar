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
package org.apache.pulsar.client.admin;

import lombok.extern.slf4j.Slf4j;

/**
 * Pulsar admin exceptions.
 */
@SuppressWarnings("serial")
@Slf4j
public class PulsarAdminException extends Exception {
    private static final int DEFAULT_STATUS_CODE = 500;

    private final String httpError;
    private final int statusCode;

    public PulsarAdminException(Throwable t, String httpError, int statusCode) {
        super(t);
        this.httpError = httpError;
        this.statusCode = statusCode;
    }

    public PulsarAdminException(String message, Throwable t, String httpError, int statusCode) {
        super(message, t);
        this.httpError = httpError;
        this.statusCode = statusCode;
    }

    public PulsarAdminException(Throwable t) {
        super(t);
        httpError = null;
        statusCode = DEFAULT_STATUS_CODE;
    }

    public PulsarAdminException(String message, Throwable t) {
        super(message, t);
        httpError = null;
        statusCode = DEFAULT_STATUS_CODE;
    }

    public PulsarAdminException(String message) {
        super(message);
        httpError = null;
        statusCode = DEFAULT_STATUS_CODE;
    }

    public String getHttpError() {
        return httpError;
    }

    public int getStatusCode() {
        return statusCode;
    }

    /**
     * Not Authorized Exception.
     */
    public static class NotAuthorizedException extends PulsarAdminException {
        public NotAuthorizedException(Throwable t, String httpError, int statusCode) {
            super(httpError, t, httpError, statusCode);
        }
    }

    /**
     * Not Found Exception.
     */
    public static class NotFoundException extends PulsarAdminException {
        public NotFoundException(Throwable t, String httpError, int statusCode) {
            super(httpError, t, httpError, statusCode);
        }
    }

    /**
     * Not Allowed Exception.
     */
    public static class NotAllowedException extends PulsarAdminException {
        public NotAllowedException(Throwable t, String httpError, int statusCode) {
            super(httpError, t, httpError, statusCode);
        }
    }

    /**
     * Conflict Exception.
     */
    public static class ConflictException extends PulsarAdminException {
        public ConflictException(Throwable t, String httpError, int statusCode) {
            super(httpError, t, httpError, statusCode);
        }
    }

    /**
     * Precondition Failed Exception.
     */
    public static class PreconditionFailedException extends PulsarAdminException {
        public PreconditionFailedException(Throwable t, String httpError, int statusCode) {
            super(httpError, t, httpError, statusCode);
        }
    }

    /**
     * Timeout Exception.
     */
    public static class TimeoutException extends PulsarAdminException {
        public TimeoutException(Throwable t) {
            super(t);
        }
    }

    /**
     * Server Side Error Exception.
     */
    public static class ServerSideErrorException extends PulsarAdminException {
        public ServerSideErrorException(Throwable t, String message, String httpError, int statusCode) {
            super(message, t, httpError, statusCode);
        }

        public ServerSideErrorException(Throwable t) {
            super("Some error occourred on the server", t);
        }
    }

    /**
     * Http Error Exception.
     */
    public static class HttpErrorException extends PulsarAdminException {
        public HttpErrorException(Exception e) {
            super(e);
        }

        public HttpErrorException(Throwable t) {
            super(t);
        }
    }

    /**
     * Connect Exception.
     */
    public static class ConnectException extends PulsarAdminException {
        public ConnectException(Throwable t) {
            super(t);
        }

        public ConnectException(String message, Throwable t) {
            super(message, t);
        }
    }

    /**
     * Getting Authentication Data Exception.
     */
    public static class GettingAuthenticationDataException extends PulsarAdminException {
        public GettingAuthenticationDataException(Throwable t) {
            super(t);
        }

        public GettingAuthenticationDataException(String msg) {
            super(msg);
        }
    }
}
