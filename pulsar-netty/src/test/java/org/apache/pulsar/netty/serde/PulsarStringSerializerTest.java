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
package org.apache.pulsar.netty.serde;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Tests for Pulsar String Serializer
 */
public class PulsarStringSerializerTest {

    @Test
    public void testPulsarStringSerializer() throws IOException {
        String expectedContent = "This is the test content";
        PulsarStringSerializer pulsarStringSerializer = new PulsarStringSerializer();
        byte[] bytes = pulsarStringSerializer.serialize(expectedContent);
        String actualContent = IOUtils.toString(bytes, StandardCharsets.UTF_8.toString());
        assertEquals(expectedContent, actualContent);
    }

}
