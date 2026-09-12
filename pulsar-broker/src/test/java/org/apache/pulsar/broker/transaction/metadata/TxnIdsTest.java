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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.testng.annotations.Test;

public class TxnIdsTest {

    @Test
    public void roundTripPositive() {
        TxnID id = new TxnID(1, 2);
        assertThat(TxnIds.toKey(id)).isEqualTo("1_2");
        assertThat(TxnIds.fromKey("1_2")).isEqualTo(id);
    }

    @Test
    public void roundTripNegativeMostSigBits() {
        TxnID id = new TxnID(-1, 1);
        assertThat(TxnIds.toKey(id)).isEqualTo("-1_1");
        assertThat(TxnIds.fromKey("-1_1")).isEqualTo(id);
    }

    @Test
    public void roundTripBothNegative() {
        TxnID id = new TxnID(-7, -42);
        assertThat(TxnIds.toKey(id)).isEqualTo("-7_-42");
        assertThat(TxnIds.fromKey("-7_-42")).isEqualTo(id);
    }

    @Test
    public void roundTripExtremes() {
        TxnID a = new TxnID(Long.MIN_VALUE, Long.MAX_VALUE);
        TxnID b = new TxnID(Long.MAX_VALUE, Long.MIN_VALUE);
        assertThat(TxnIds.fromKey(TxnIds.toKey(a))).isEqualTo(a);
        assertThat(TxnIds.fromKey(TxnIds.toKey(b))).isEqualTo(b);
    }

    @Test
    public void fromKeyRejectsMissingSeparator() {
        assertThatThrownBy(() -> TxnIds.fromKey("nope"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void fromKeyRejectsTrailingSeparator() {
        assertThatThrownBy(() -> TxnIds.fromKey("1_"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void fromKeyRejectsExtraSeparators() {
        assertThatThrownBy(() -> TxnIds.fromKey("1_2_3"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
