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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.testng.annotations.Test;

public class TxnPathsTest {

    @Test
    public void ackIndexSegmentBoundsCoverTheAckKeysOfTheSegmentOnly() {
        String segment = "segment://public/default/topic/0000-7fff-1";
        String lower = TxnPaths.ackIndexSegmentLowerBound(segment);
        String upper = TxnPaths.ackIndexSegmentUpperBound(segment);
        // Subscription names with the separator, with '~', and with characters that sort high or low.
        for (String subscription : List.of("sub", "a", "zzz", "~", "~~", "sub:x", "sub/x", "a b", "100%")) {
            assertThat(TxnPaths.ackIndexKey(segment, subscription))
                    .as("ack index key of subscription %s", subscription)
                    .isBetween(lower, upper);
        }
        // The segments of the topic with the same hash range whose names sort next to this one's, the first one
        // extending it.
        assertThat(TxnPaths.ackIndexKey("segment://public/default/topic/0000-7fff-12", "sub")).isLessThan(lower);
        assertThat(TxnPaths.ackIndexKey("segment://public/default/topic/0000-7fff-0", "sub")).isLessThan(lower);
        assertThat(TxnPaths.ackIndexKey("segment://public/default/topic/0000-7fff-2", "sub")).isGreaterThan(upper);
    }
}
