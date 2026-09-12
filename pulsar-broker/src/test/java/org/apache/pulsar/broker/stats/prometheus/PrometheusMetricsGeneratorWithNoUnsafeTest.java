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
package org.apache.pulsar.broker.stats.prometheus;

import static org.testng.Assert.assertFalse;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.time.Clock;
import lombok.Cleanup;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-isolated")
public class PrometheusMetricsGeneratorWithNoUnsafeTest {

    @BeforeClass
    static void setup() {
        System.setProperty("io.netty.noUnsafe", "true");
    }

    @Test
    public void testWriteStringWithNoUnsafe() {
        assertFalse(PlatformDependent.hasUnsafe());
        @Cleanup
        PrometheusMetricsGenerator generator = new PrometheusMetricsGenerator(null, false, false, false, false,
            Clock.systemUTC());
        @Cleanup("release")
        ByteBuf buf = generator.allocateMultipartCompositeDirectBuffer();
        for (int i = 0; i < 2; i++) {
            buf.writeBytes(new byte[1024 * 1024]);
        }
        SimpleTextOutputStream outputStream = new SimpleTextOutputStream(buf);
        outputStream.write("test");
    }
}
