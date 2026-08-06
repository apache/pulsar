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
 * v5-native implementations of the built-in credential-fetching authentication plugins (PIP-478), and
 * the shared {@link org.apache.pulsar.client.impl.auth.v5.V5BinaryAuthenticationDriver} that exposes
 * them to {@code ClientCnx} through the {@code AsyncAuthenticationDriver} carve-out. The v4 plugin
 * classes in the parent package are thin shims that keep their verbatim synchronous surface and drive
 * these bodies on the async binary path.
 */
package org.apache.pulsar.client.impl.auth.v5;
