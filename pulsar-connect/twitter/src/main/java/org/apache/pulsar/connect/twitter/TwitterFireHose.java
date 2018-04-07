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

package org.apache.pulsar.connect.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.pulsar.connect.core.PushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.common.DelimitedStreamReader;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import static org.apache.pulsar.connect.twitter.TwitterFireHoseConfigKeys.*;

/**
 * Simple Push based Twitter FireHose Source
 */
public class TwitterFireHose implements PushSource<String> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterFireHose.class);

    // ----- Fields set by the constructor

    // ----- Runtime fields
    private Object waitObject;
    private Function<String, CompletableFuture<Void>> consumeFunction;

    @Override
    public void open(Map<String, String> config) {
        verifyExists(config, CONSUMER_KEY);
        verifyExists(config, CONSUMER_SECRET);
        verifyExists(config, TOKEN);
        verifyExists(config, TOKEN_SECRET);
        waitObject = new Object();
        startThread(config);
    }

    @Override
    public void setConsumer(Function<String, CompletableFuture<Void>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void close() throws Exception {
        stopThread();
    }


    private void verifyExists(final Map<String, String> config, final String key) {
        if (!config.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }
    // ------ Custom endpoints

    /**
     * Implementing this interface allows users of this source to set a custom endpoint.
     */
    public interface EndpointInitializer {
        StreamingEndpoint createEndpoint();
    }

    /**
     * Required for Twitter Client
     */
    private static class SampleStatusesEndpoint implements EndpointInitializer, Serializable {
        @Override
        public StreamingEndpoint createEndpoint() {
            // this default endpoint initializer returns the sample endpoint: Returning a sample from the firehose (all tweets)
            StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
            endpoint.stallWarnings(false);
            endpoint.delimited(false);
            return endpoint;
        }
    }

    private void startThread(Map<String, String> config) {
        Authentication auth = new OAuth1(config.get(CONSUMER_KEY),
                config.get(CONSUMER_SECRET),
                config.get(TOKEN),
                config.get(TOKEN_SECRET));

        BasicClient client = new ClientBuilder()
                .name(config.getOrDefault(CLIENT_NAME, "openconnector-twitter-source"))
                .hosts(config.getOrDefault(CLIENT_HOSTS, Constants.STREAM_HOST))
                .endpoint(new SampleStatusesEndpoint().createEndpoint())
                .authentication(auth)
                .processor(new HosebirdMessageProcessor() {
                    public DelimitedStreamReader reader;

                    @Override
                    public void setup(InputStream input) {
                        reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET,
                            Integer.parseInt(config.getOrDefault(CLIENT_BUFFER_SIZE, "50000")));
                    }

                    @Override
                    public boolean process() throws IOException, InterruptedException {
                        String line = reader.readLine();
                        try {
                            consumeFunction.apply(line);
                        } catch (Exception e) {
                            LOG.error("Exception thrown");
                        }
                        return true;
                    }
                })
                .build();

        Thread runnerThread = new Thread(() -> {
            LOG.info("Started the Twitter FireHose Runner Thread");
            client.connect();
            LOG.info("Twitter Streaming API connection established successfully");

            // just wait now
            try {
                synchronized (waitObject) {
                    waitObject.wait();
                }
            } catch (Exception e) {
                LOG.info("Got a exception in waitObject");
            }
            LOG.debug("Closing Twitter Streaming API connection");
            client.stop();
            LOG.info("Twitter Streaming API connection closed");
            LOG.info("Twitter FireHose Runner Thread ending");
        });
        runnerThread.setName("TwitterFireHoseRunner");
        runnerThread.start();
    }

    private void stopThread() {
        LOG.info("Source closed");
        synchronized (waitObject) {
            waitObject.notify();
        }
    }

}
