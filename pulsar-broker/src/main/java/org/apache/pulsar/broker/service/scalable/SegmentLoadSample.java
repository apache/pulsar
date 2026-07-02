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
package org.apache.pulsar.broker.service.scalable;

import org.apache.pulsar.common.scalable.SegmentLoadStats;

/**
 * A segment's load record as the controller sees it: the persisted {@link SegmentLoadStats}
 * plus the metadata store's last-modified timestamp for the record (PIP-483).
 *
 * <p>This is an in-memory evaluator input, never persisted — the timestamp comes from the
 * metadata {@code Stat}, not from the stored value. {@code modifiedAtMs} is what the merge
 * pass uses to require a segment has stayed cold for at least {@code mergeWindow}.
 *
 * @param stats        the persisted rates
 * @param modifiedAtMs metadata-store last-modified time of the load record, in epoch millis
 */
public record SegmentLoadSample(SegmentLoadStats stats, long modifiedAtMs) {
}
