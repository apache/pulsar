/**
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
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class TripleLongPriorityQueueTest {

    @Test
    public void testQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int N = 20;

        for (int i = N; i > 0; i--) {
            pq.add(i, i * 2, i * 3);
        }

        assertEquals(pq.size(), N);

        for (int i = 1; i <= N; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), N - i);
        }

        pq.close();
    }
}
