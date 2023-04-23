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
package org.apache.pulsar.broker.delayed.bucket;

import java.util.Comparator;
import java.util.Objects;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;

interface DelayedIndexQueue {
    Comparator<DelayedMessageIndexBucketSnapshotFormat.DelayedIndex> COMPARATOR = (o1, o2) ->  {
        if (!Objects.equals(o1.getTimestamp(), o2.getTimestamp())) {
            return Long.compare(o1.getTimestamp(), o2.getTimestamp());
        } else if (!Objects.equals(o1.getLedgerId(), o2.getLedgerId())) {
            return Long.compare(o1.getLedgerId(), o2.getLedgerId());
        } else {
            return Long.compare(o1.getEntryId(), o2.getEntryId());
        }
    };

    boolean isEmpty();

    DelayedMessageIndexBucketSnapshotFormat.DelayedIndex peek();

    DelayedMessageIndexBucketSnapshotFormat.DelayedIndex pop();
}
