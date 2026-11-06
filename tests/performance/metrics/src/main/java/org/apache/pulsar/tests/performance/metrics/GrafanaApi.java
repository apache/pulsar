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
package org.apache.pulsar.tests.performance.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Base64;
import java.util.List;

/**
 * The metrics stack's Grafana API, as its admin user: adds annotations, and renders panels as PNG images with the
 * image renderer, which renders no SVG.
 */
public final class GrafanaApi {
    /** The tag of the runs' annotations, which the dashboards' annotation query shows. */
    public static final String PULSAR_PERFORMANCE_TAG = "pulsar-performance";
    private static final Duration RENDER_TIMEOUT = Duration.ofSeconds(90);

    private final String baseUrl;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String authorization = "Basic " + Base64.getEncoder().encodeToString(
            (MetricsStack.GRAFANA_USER + ":" + MetricsStack.GRAFANA_PASSWORD).getBytes(StandardCharsets.UTF_8));

    /** @param baseUrl Grafana's URL on this host, ending in {@code /} */
    public GrafanaApi(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Adds an annotation to every dashboard, which their annotation query of the runs shows.
     *
     * @param tags the annotation's tags besides {@code pulsar-performance}
     */
    public void annotate(Instant time, List<String> tags, String text) throws IOException, InterruptedException {
        ObjectNode annotation = mapper.createObjectNode();
        annotation.put("time", time.toEpochMilli());
        annotation.putArray("tags").add(PULSAR_PERFORMANCE_TAG).addAll(
                tags.stream().map(mapper.getNodeFactory()::textNode).toList());
        annotation.put("text", text);
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(baseUrl + "api/annotations"))
                .timeout(Duration.ofSeconds(10)).header("Authorization", authorization)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(annotation))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Grafana didn't add the annotation: " + response.statusCode() + " "
                    + response.body());
        }
    }

    /** Renders a panel of a run as a PNG image. */
    public byte[] render(Grafana.Panel panel, String cluster, Instant from, Instant to, int width, int height)
            throws IOException, InterruptedException {
        HttpResponse<byte[]> response = client.send(HttpRequest.newBuilder(
                        URI.create(renderUrl(baseUrl, panel, cluster, from, to, width, height)))
                .timeout(RENDER_TIMEOUT).header("Authorization", authorization).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        String contentType = response.headers().firstValue("Content-Type").orElse("");
        if (response.statusCode() != 200 || !contentType.startsWith("image/png")) {
            throw new IOException("Grafana didn't render the panel " + panel.title() + ": " + response.statusCode()
                    + " " + contentType);
        }
        return response.body();
    }

    /**
     * The URL that renders a panel of a run, in the light theme and this host's time zone, as the run report's own
     * charts are.
     */
    static String renderUrl(String baseUrl, Grafana.Panel panel, String cluster, Instant from, Instant to,
                            int width, int height) {
        return baseUrl + "render/d-solo/" + panel.dashboard().path() + "?"
                + Grafana.query(panel.dashboard(), cluster, from, to) + "&panelId=" + panel.panelId()
                + "&width=" + width + "&height=" + height + "&theme=light&tz="
                + URLEncoder.encode(ZoneId.systemDefault().getId(), StandardCharsets.UTF_8);
    }
}
