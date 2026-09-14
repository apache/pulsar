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
package org.apache.pulsar.tls;

/**
 * The destination of an outbound connection, passed to a {@link PulsarTlsFactory} as a per-request
 * hint (PIP-478).
 *
 * <p>Client-side consumers pass the target host and port when they know it, so a factory that serves
 * per-destination material (multi-cluster deployments, per-target workload identities) may specialize
 * on it. Factories are free to ignore it — the default file-based factory does. The endpoint does NOT
 * replace hostname verification or SNI: those are applied at engine creation from the same peer
 * address by whichever component builds the {@code SSLEngine}.
 *
 * @param host the destination host
 * @param port the destination port
 */
public record TlsEndpoint(String host, int port) {
}
