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
package org.apache.pulsar.broker.loadbalance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import java.nio.file.Files;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LinuxInfoUtilsTest {

    /**
     * simulate reading first line of /proc/stat to get total cpu usage.
     */
    @Test
    public void testGetCpuUsageForEntireHost(){
        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.lines(any())).thenReturn(
                    Stream.generate(() -> "cpu  317808 128  58637  2503692 7634   0   13472   0    0     0"));
            long idle = 2503692 + 7634, total = 2901371;
            LinuxInfoUtils.ResourceUsage resourceUsage = LinuxInfoUtils.ResourceUsage.builder()
                    .usage(total - idle)
                    .idle(idle)
                    .total(total).build();
            assertEquals(LinuxInfoUtils.getCpuUsageForEntireHost(), resourceUsage);
        }
    }
}
