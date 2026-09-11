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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import org.testng.annotations.Test;

public class ScalableSubscriptionTypeTest {

    @Test
    public void testExpectedValuesExist() {
        // The wire protocol + admin API identify these by name; a rename or reorder is a
        // breaking change. This test locks the enum down to its two current values.
        ScalableSubscriptionType[] values = ScalableSubscriptionType.values();
        assertEquals(values.length, 2);
        assertEquals(values[0], ScalableSubscriptionType.STREAM);
        assertEquals(values[1], ScalableSubscriptionType.QUEUE);
    }

    @Test
    public void testValueOfRoundtrip() {
        assertEquals(ScalableSubscriptionType.valueOf("STREAM"), ScalableSubscriptionType.STREAM);
        assertEquals(ScalableSubscriptionType.valueOf("QUEUE"), ScalableSubscriptionType.QUEUE);
    }

    @Test
    public void testValueOfRejectsUnknown() {
        assertThrows(IllegalArgumentException.class,
                () -> ScalableSubscriptionType.valueOf("DOES_NOT_EXIST"));
    }
}
