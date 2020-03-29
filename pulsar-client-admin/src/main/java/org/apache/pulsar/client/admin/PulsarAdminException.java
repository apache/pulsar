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

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Pulsar admin exceptions.
 */
@SuppressWarnings("serial")
@Slf4j
public class PulsarAdminException extends Exception {
    private static final int DEFAULT_STATUS_CODE = 500;

    private final String httpError;
    private final int statusCode;

    private static String getReasonFromServer(WebApplicationException e) {
        try {
            return e.getResponse().readEntity(ErrorData.class).reason;
        } catch (Exception ex) {
            try {
                return ObjectMapperFactory.getThreadLocal().readValue(
                        e.getResponse().getEntity().toString(), ErrorData.class).reason;
            } catch (Exception ex1) {
                try {
                    return ObjectMapperFactory.getThreadLocal().readValue(e.getMessage(), ErrorData.class).reason;
                } catch (Exception ex2) {
                    // could not parse output to ErrorData class
                    return e.getMessage();
                }
            }
        }
    }

    public PulsarAdminException(ClientErrorException e) {
        super(getReasonFromServer(e), e);
        this.httpError = getReasonFromServer(e);
        this.statusCode = e.getResponse().getStatus();
    }

    public PulsarAdminException(ClientErrorException e, String message) {
        super(message, e);
        this.httpError = getReasonFromServer(e);
        this.statusCode = e.getResponse().getStatus();
    }

    public PulsarAdminException(ServerErrorException e) {
        super(getReasonFromServer(e), e);
        this.httpError = getReasonFromServer(e);
        this.statusCode = e.getResponse().getStatus();
    }

    public PulsarAdminException(ServerErrorException e, String message) {
        super(message, e);
        this.httpError = getReasonFromServer(e);
        this.statusCode = e.getResponse().getStatus();
    }

    public PulsarAdminException(Throwable t) {
        super(t);
        httpError = null;
        statusCode = DEFAULT_STATUS_CODE;
    }

    public PulsarAdminException(WebApplicationException e) {
        super(getReasonFromServer(e), e);
        this.httpError = getReasonFromServer(e);
        this.statusCode = e.getResponse().getStatus();
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
        public NotAuthorizedException(ClientErrorException e) {
            super(e);
        }
    }

    /**
     * Not Found Exception.
     */
    public static class NotFoundException extends PulsarAdminException {
        public NotFoundException(ClientErrorException e) {
            super(e);
        }
    }

    /**
     * Not Allowed Exception.
     */
    public static class NotAllowedException extends PulsarAdminException {
        public NotAllowedException(ClientErrorException e) {
            super(e);
        }
    }

    /**
     * Conflict Exception.
     */
    public static class ConflictException extends PulsarAdminException {
        public ConflictException(ClientErrorException e) {
            super(e);
        }
    }

    /**
     * Precondition Failed Exception.
     */
    public static class PreconditionFailedException extends PulsarAdminException {
        public PreconditionFailedException(ClientErrorException e) {
            super(e);
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
        public ServerSideErrorException(ServerErrorException e, String msg) {
            super(e, msg);
        }

        public ServerSideErrorException(ServerErrorException e) {
            super(e, "Some error occourred on the server");
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
