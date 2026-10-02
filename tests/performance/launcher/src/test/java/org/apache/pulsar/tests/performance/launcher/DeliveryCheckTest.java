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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

public class DeliveryCheckTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void findsDuplicatesOrderingViolationsAndInvalidMessages() throws Exception {
        JsonNode workload = mapper.readTree("{\"applications\": {\"subscriptionPrefix\": \"iot-application-\"}}");

        assertThat(deliveredIncorrectly(workload, "{\"duplicates\": 0, \"orderingViolations\": 0}")).isFalse();
        assertThat(deliveredIncorrectly(workload, "{\"duplicates\": 33377}")).isTrue();
        assertThat(deliveredIncorrectly(workload, "{\"orderingViolations\": 1}")).isTrue();
        assertThat(deliveredIncorrectly(workload, "{\"invalidMessages\": 2}")).isTrue();
    }

    // The first application received every message correctly, the second one the given summary
    private boolean deliveredIncorrectly(JsonNode workload, String secondSummary) throws Exception {
        Path run = Files.createTempDirectory("delivery-check");
        write(run.resolve("applications/iot-application-0/application-summary.json"), "{\"duplicates\": 0}");
        write(run.resolve("applications/iot-application-1/application-summary.json"), secondSummary);
        return PerformanceLauncher.deliveredIncorrectly(mapper, run, workload, 2);
    }

    private static void write(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }
}
