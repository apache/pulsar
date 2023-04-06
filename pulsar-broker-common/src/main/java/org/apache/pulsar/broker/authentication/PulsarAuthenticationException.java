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
package org.apache.pulsar.broker.authentication;

import javax.naming.AuthenticationException;
import lombok.Getter;

/**
 * PulsarAuthenticationException indicates Authentication exception in Pulsar, it contains an error code.
 */
public class PulsarAuthenticationException extends AuthenticationException {

    public enum ErrorCode {
        UNKNOWN,
        PROVIDER_LIST_AUTH_REQUIRED,
        BASIC_INVALID_TOKEN,
        BASIC_INVALID_AUTH_DATA,
        AUTHZ_NO_CLIENT,
        AUTHZ_NO_TOKEN,
        AUTHZ_NO_PUBLIC_KEY,
        AUTHZ_DOMAIN_MISMATCH,
        AUTHZ_INVALID_TOKEN,
        TLS_NO_CERTS,
        TLS_NO_CN, // cn: common name
        TOKEN_INVALID_HEADER,
        TOKEN_NO_AUTH_DATA,
        TOKEN_EMPTY_TOKEN,
        TOKEN_INVALID_TOKEN,
        TOKEN_INVALID_AUDIENCES,
    }

    @Getter
    private final ErrorCode errorCode;

    public PulsarAuthenticationException(String message, ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN;
    }

}
