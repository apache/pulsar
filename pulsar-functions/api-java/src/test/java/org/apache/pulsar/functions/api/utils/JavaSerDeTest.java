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
package org.apache.pulsar.functions.api.utils;

import static org.testng.Assert.assertEquals;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.testng.annotations.Test;

/**
 * Unit test of {@link JavaSerDe}.
 */
public class JavaSerDeTest {

    @Data
    @AllArgsConstructor
    private static class TestObject implements Serializable {

        private int intField;
        private String stringField;

    }

    @Test
    public void testSerDe() {
        TestObject to = new TestObject(1234, "test-serde-java-object");

        byte[] data = JavaSerDe.of().serialize(to);
        TestObject deserializeTo = (TestObject) JavaSerDe.of().deserialize(data);

        assertEquals(to, deserializeTo);
    }

}
