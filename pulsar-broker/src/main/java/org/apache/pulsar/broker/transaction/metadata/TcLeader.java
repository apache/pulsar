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
package org.apache.pulsar.broker.transaction.metadata;

/**
 * Value stored in a per-partition transaction-coordinator leader-election node
 * ({@code /txn/tc/leader/<partition>}). Identifies the broker currently coordinating that TC
 * partition and carries the connection URLs a client needs to reach it — so any broker can
 * answer a client's assignment watch from the election value alone, without a further lookup.
 *
 * <p>Serialized as JSON via {@link org.apache.pulsar.common.util.ObjectMapperFactory} by the
 * coordination service's {@code LeaderElection} serde.
 *
 * @param brokerId            the elected broker's id (matches the {@code /loadbalance/brokers} key)
 * @param brokerServiceUrl    the broker's binary service URL (non-TLS); may be {@code null} if the
 *                            broker only advertises a TLS endpoint
 * @param brokerServiceUrlTls the broker's binary service URL (TLS); may be {@code null} if TLS is
 *                            disabled
 * @param webServiceUrl       the broker's HTTP service URL, for admin/CLI resolution
 */
public record TcLeader(String brokerId, String brokerServiceUrl, String brokerServiceUrlTls,
                       String webServiceUrl) {
}
