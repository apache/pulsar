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
package org.apache.pulsar.common.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SslContextConf {
    private Supplier<X509Certificate[]> trustCertSupplier;
    private Supplier<PrivateKey> keySupplier;
    private Supplier<X509Certificate[]> certSupplier;

    private String sslProvider; // java.security.Provider
    private String sslContextProvider; // io.netty.handler.ssl.SslProvider

    private Set<String> ciphers;
    private Set<String> protocols;

    private long refreshIntervalSeconds;

    private boolean allowInsecureConnection;

    private boolean requireTrustedClientCertOnConnect;

    private boolean forClient;
}
