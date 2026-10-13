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
package org.apache.pulsar.tests.performance.tools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class JoinPreparedMarkerTest {
    @Test
    public void carriesTheMessagesThatTheApplicationsExpect() throws Exception {
        Path directory = Files.createTempDirectory("join-prepared");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        JoinPreparedMarker.mark(directory, "run-1", Map.of(1, 104_877L, 4, 33_110L));
        assertThat(JoinPreparedMarker.await(directory, "run-1", deadline))
                .containsExactlyInAnyOrderEntriesOf(Map.of(1, 104_877L, 4, 33_110L));
        JoinPreparedMarker.mark(directory, "run-2");
        assertThat(JoinPreparedMarker.await(directory, "run-2", deadline)).isEmpty();
        assertThatThrownBy(() -> JoinPreparedMarker.await(directory, "run-3", System.nanoTime()))
                .isInstanceOf(TimeoutException.class);
    }
}
