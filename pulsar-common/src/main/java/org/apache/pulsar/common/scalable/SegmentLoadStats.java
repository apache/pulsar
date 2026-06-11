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
package org.apache.pulsar.common.scalable;

/**
 * Per-segment load sample for scalable-topic auto split/merge (PIP-483).
 *
 * <p>Written by the broker that owns a segment's {@code segment://} topic, directly to the
 * metadata store under {@code .../segments/{segmentId}/load}, and read by the controller
 * leader's auto-scaling evaluator. To keep write volume bounded, the owning broker only
 * rewrites this record when one of the rates changes by more than a significant threshold
 * (default ±25%) since the last write — see {@code SegmentLoadReporter}.
 *
 * <p>The record carries no timestamp of its own: the metadata store exposes the record's
 * last-modified time via its {@code Stat}, and the controller uses that for the "cold for
 * at least mergeWindow" check.
 *
 * @param msgRateIn    inbound messages per second (60s rolling average, from {@code TopicStats})
 * @param bytesRateIn  inbound bytes per second
 * @param msgRateOut   outbound (dispatched) messages per second, summed across subscriptions
 * @param bytesRateOut outbound bytes per second
 */
public record SegmentLoadStats(
        double msgRateIn,
        double bytesRateIn,
        double msgRateOut,
        double bytesRateOut
) {
    public static final SegmentLoadStats ZERO = new SegmentLoadStats(0, 0, 0, 0);
}
