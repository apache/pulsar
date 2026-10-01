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
package org.apache.pulsar.broker.resources;

/**
 * Persisted marker of a consumer registered against a subscription on a scalable topic.
 *
 * <p>The znode <em>existence</em> is the durable session — it survives TCP disconnects,
 * client restarts, and controller leader failovers. The consumer name lives in the znode
 * path
 * ({@code /topics/{tenant}/{namespace}/{topic}/subscriptions/{subscription}/consumers/{consumerName}})
 * so no fields are stored in the value.
 *
 * <p>Keep-alive state (connected/disconnected, grace-period timer) is <em>not</em>
 * persisted — it is tracked in-memory by the controller leader only. A new leader taking
 * over reads these entries and starts a fresh grace-period timer for each consumer.
 *
 * <p>The current segment assignment is <em>not</em> persisted either: since assignment is
 * a pure function of the sorted consumer names and the current segment layout, the new
 * leader recomputes it deterministically after loading the registrations.
 */
public record ConsumerRegistration() {}
