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
package org.apache.pulsar.broker.authentication;

import lombok.Builder;

/**
 * A class to collect all the common fields used for authentication. Because the authentication data source is
 * not always consistent when using the Pulsar Protocol and the Pulsar Proxy
 * (see <a href="https://github.com/apache/pulsar/issues/19332">19332</a>), this class is currently restricted
 * to use only in authenticating HTTP requests.
 */
@Builder
@lombok.Value
public class AuthenticationParameters {

    /**
     * The original principal (or role) of the client.
     * <p>
     *     For HTTP Authentication, there are two possibilities. When the client connects directly to the broker, this
     *     field is null (assuming the client hasn't supplied the original principal header). When the client connects
     *     through the proxy, the proxy calculates the original principal based on the client's authentication data and
     *     supplies it in this field.
     * </p>
     */
    String originalPrincipal;

    /**
     * The client role.
     * <p>
     *     For HTTP Authentication, there are three possibilities. When the client connects directly to the broker, this
     *     is the client's role. When the client connects through the proxy using mTLS, this is the role of the proxy.
     *     In this case, the {@link #originalPrincipal} is also supplied and is the role of the original client as
     *     determined by the proxy. When the client connects through the proxy using any other form of authentication,
     *     this is the role of the original client. In this case, the {@link #originalPrincipal} is also the role of
     *     the original client.
     * </p>
     */
    String clientRole;

    /**
     * The authentication data source used to generate the {@link #clientRole}.
     * <p>
     *     For HTTP Authentication, there are three possibilities. When the client connects directly to the broker, this
     *     is the client's {@link AuthenticationDataSource}. When the client connects through the proxy using mTLS, this
     *     is the proxy's {@link AuthenticationDataSource}. When the client connects through the proxy using any other
     *     form of authentication, this is the original client's {@link AuthenticationDataSource}.
     * </p>
     */
    AuthenticationDataSource clientAuthenticationDataSource;
}
