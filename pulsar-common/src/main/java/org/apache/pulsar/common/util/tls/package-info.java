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

/**
 * Cohesive, single-concern TLS utility containers (PIP-478). Each class holds one concern rather than the
 * former {@code SecurityUtility} grab-bag: {@link org.apache.pulsar.common.util.tls.PemReader} parses PEM
 * certificates and keys, {@link org.apache.pulsar.common.util.tls.JcaProviders} resolves JCA/JCE security
 * providers, and {@link org.apache.pulsar.common.util.tls.JdkSslContexts} assembles JDK
 * {@code javax.net.ssl.SSLContext} instances from loaded material.
 */
package org.apache.pulsar.common.util.tls;
