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
package org.apache.pulsar.exception;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PulsarExceptionTest {

    @Test
    public void testPulsarExceptionWithParameters() {

        String key = "123";
        String value = "abc";

        PulsarExceptionSample testException =
                new PulsarExceptionSample("key={}, value={}", key, value);
        assertThat(testException.getMessage()).isEqualTo("key=123, value=abc");
    }

    @Test
    public void testPulsarExceptionSimple() {

        PulsarExceptionSample testException =
                new PulsarExceptionSample("Simple Message");
        assertThat(testException.getMessage()).isEqualTo("Simple Message");
    }

    @Test
    public void testPulsarExceptionCompound() {

        int key = 123;
        String value = "abc";

        PulsarExceptionSample testException =
                new PulsarExceptionSample("key={}, value={}", key, value, new Exception("inner exception"));

        assertThat(testException.getMessage()).contains("key=123, value=abc");
        assertThat(testException.getCause().getMessage()).isEqualTo("inner exception");
    }
}
