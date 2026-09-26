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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class MeasurementControlTest {
    private final HttpClient client = HttpClient.newHttpClient();

    @Test(timeOut = 30_000)
    public void answersReadyOnceTheProducerIsReadyAndReleasesTheStart() throws Exception {
        try (MeasurementControl control = MeasurementControl.start(0)) {
            // Not ready yet: the wait runs out and the answer says so
            assertThat(get(control, MeasurementControl.READY_PATH + "?waitMillis=50").statusCode()).isEqualTo(204);

            // A waiting request is answered as soon as the producer is ready
            CompletableFuture<HttpResponse<String>> waiting = client.sendAsync(
                    request(control, MeasurementControl.READY_PATH + "?waitMillis=5000").GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            Thread.sleep(100);
            control.markReady();
            assertThat(waiting.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);

            CompletableFuture<Void> started = CompletableFuture.runAsync(() -> {
                try {
                    control.awaitStart(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            assertThat(get(control, MeasurementControl.START_PATH).statusCode()).isEqualTo(405);
            assertThat(client.send(request(control, MeasurementControl.START_PATH)
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString())
                    .statusCode()).isEqualTo(200);
            started.get(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 30_000)
    public void failsTheProducerWhenTheLauncherNeverStartsTheMeasurement() throws Exception {
        try (MeasurementControl control = MeasurementControl.start(0)) {
            assertThatThrownBy(() -> control.awaitStart(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Timed out");
        }
    }

    private HttpResponse<String> get(MeasurementControl control, String path) throws Exception {
        return client.send(request(control, path).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpRequest.Builder request(MeasurementControl control, String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + control.port() + path))
                .timeout(Duration.ofSeconds(20));
    }
}
