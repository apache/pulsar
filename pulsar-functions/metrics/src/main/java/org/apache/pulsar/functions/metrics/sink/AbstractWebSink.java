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
package org.apache.pulsar.functions.metrics.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.sun.net.httpserver.HttpServer;

import org.apache.pulsar.functions.metrics.MetricsSink;


/**
 * A metrics sink that publishes metrics on a http endpoint
 */
abstract public class AbstractWebSink implements MetricsSink {
    private static final Logger LOG = Logger.getLogger(AbstractWebSink.class.getName());

    private static final int HTTP_STATUS_OK = 200;

    // Metrics will be published on http://host:port/path, the port
    public static final String KEY_PORT = "port";

    // The path
    public static final String KEY_PATH = "path";

    // Maximum number of metrics getting served
    private static final String KEY_METRICS_CACHE_MAX_SIZE = "metrics-cache-max-size";
    private static final String DEFAULT_MAX_CACHE_SIZE = "1000000";

    // Time To Live before a metric gets evicted from the cache
    private static final String KEY_METRICS_CACHE_TTL_SEC = "metrics-cache-ttl-sec";
    private static final String DEFAULT_CACHE_TTL_SECONDS = "600";

    private HttpServer httpServer;
    private long cacheMaxSize;
    private long cacheTtlSeconds;
    private final Ticker cacheTicker;

    AbstractWebSink() {
        this(Ticker.systemTicker());
    }

    @VisibleForTesting
    AbstractWebSink(Ticker cacheTicker) {
        this.cacheTicker = cacheTicker;
    }

    @Override
    public final void init(Map<String, String> conf) {
        String path = conf.get(KEY_PATH);

        cacheMaxSize = Long.valueOf(conf.getOrDefault(KEY_METRICS_CACHE_MAX_SIZE,
                DEFAULT_MAX_CACHE_SIZE));

        cacheTtlSeconds = Long.valueOf(conf.getOrDefault(KEY_METRICS_CACHE_TTL_SEC,
                DEFAULT_CACHE_TTL_SECONDS));

        // initialize child classes
        initialize(conf);

        int port = Integer.valueOf(conf.getOrDefault(KEY_PORT, "9099"));
        startHttpServer(path, port);
    }

    /**
     * Start a http server on supplied port that will serve the metrics, as json,
     * on the specified path.
     *
     * @param path
     * @param port
     */
    protected void startHttpServer(String path, int port) {
        LOG.info("Starting AbstractWebMetricSink at path" + path + " and port " + port);
        try {
            httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            httpServer.createContext(path, httpExchange -> {
                byte[] response = generateResponse();
                httpExchange.sendResponseHeaders(HTTP_STATUS_OK, response.length);
                OutputStream os = httpExchange.getResponseBody();
                os.write(response);
                os.close();
                LOG.log(Level.INFO, "Received metrics request.");
            });
            httpServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Http server on port " + port, e);
        }
    }

    // a convenience method for creating a metrics cache
    <K, V> Cache<K, V> createCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(cacheMaxSize)
                .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
                .ticker(cacheTicker)
                .build();
    }

    abstract byte[] generateResponse() throws IOException;

    abstract void initialize(Map<String, String> configuration);

    @Override
    public void flush() { }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }
}
