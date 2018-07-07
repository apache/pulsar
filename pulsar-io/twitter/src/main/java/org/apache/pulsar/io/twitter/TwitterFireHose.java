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

package org.apache.pulsar.io.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.common.DelimitedStreamReader;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Push based Twitter FireHose Source
 */
public class TwitterFireHose extends PushSource<String> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterFireHose.class);

    // ----- Fields set by the constructor

    // ----- Runtime fields
    private Object waitObject;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws IOException {
        TwitterFireHoseConfig hoseConfig = TwitterFireHoseConfig.load(config);
        if (hoseConfig.getConsumerKey() == null
                || hoseConfig.getConsumerSecret() == null
                || hoseConfig.getToken() == null
                || hoseConfig.getTokenSecret() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }
        waitObject = new Object();
        startThread(hoseConfig);
    }

    @Override
    public void close() throws Exception {
        stopThread();
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

    private void startThread(TwitterFireHoseConfig config) {
        Authentication auth = new OAuth1(config.getConsumerKey(),
                config.getConsumerSecret(),
                config.getToken(),
                config.getTokenSecret());

        BasicClient client = new ClientBuilder()
                .name(config.getClientName())
                .hosts(config.getClientHosts())
                .endpoint(new SampleStatusesEndpoint().createEndpoint())
                .authentication(auth)
                .processor(new HosebirdMessageProcessor() {
                    public DelimitedStreamReader reader;

                    @Override
                    public void setup(InputStream input) {
                        reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET,
                            config.getClientBufferSize());
                    }

                    @Override
                    public boolean process() throws IOException, InterruptedException {
                        String line = reader.readLine();
                        try {
                            // We don't really care if the record succeeds or not.
                            // However might be in the future to count failures
                            // TODO:- Figure out the metrics story for connectors
                            consume(new TwitterRecord(line));
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

    static private class TwitterRecord implements Record<String> {
        private String tweet;

        public TwitterRecord(String tweet) {
            this.tweet = tweet;
        }

        @Override
        public String getValue() {
            return tweet;
        }
    }

}
