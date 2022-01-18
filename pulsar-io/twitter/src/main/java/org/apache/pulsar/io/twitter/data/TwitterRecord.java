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
package org.apache.pulsar.io.twitter.data;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import org.apache.pulsar.functions.api.Record;

/**
 * Twitter Record object.
 */
public class TwitterRecord implements Record<TweetData> {
    private final TweetData tweet;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");
    private final boolean guestimateTweetTime;

    public TwitterRecord(TweetData tweet, boolean guestimateTweetTime) {
        this.tweet = tweet;
        this.guestimateTweetTime = guestimateTweetTime;
    }

    @Override
    public Optional<String> getKey() {
        // TODO: Could use user or tweet ID as key here
        return Optional.empty();
    }

    @Override
    public Optional<Long> getEventTime() {
        try {
            if (tweet.getCreatedAt() != null) {
                Date d = dateFormat.parse(tweet.getCreatedAt());
                return Optional.of(d.toInstant().toEpochMilli());
            } else if (guestimateTweetTime) {
                return Optional.of(System.currentTimeMillis());
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public TweetData getValue() {
        return tweet;
    }
}