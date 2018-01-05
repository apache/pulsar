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
package org.apache.pulsar.functions.runtime.serde;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * Unit test of {@link Utf8StringSerDe}.
 */
public class Utf8StringSerDeTest {

    @Test
    public void testSerDe() {
        String message = "test-serde-utf8-string";

        byte[] data = Utf8StringSerDe.of().serialize(message);
        String deserializedMsg = (String) Utf8StringSerDe.of().deserialize(data);

        assertEquals(message, deserializedMsg);
    }

}
