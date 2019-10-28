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
package org.apache.pulsar.client.kafka.compat.examples.utils;

import java.io.Serializable;

import com.google.common.base.Objects;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

public class Tweet implements Serializable {
    private static final long serialVersionUID = 1L;
    public String userName;
    public String message;

    public Tweet(String userName, String message) {
        super();
        this.userName = userName;
        this.message = message;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(userName, message);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Tweet) {
            Tweet tweet = (Tweet) obj;
            return Objects.equal(this.userName, tweet.userName) && Objects.equal(this.message, tweet.message);
        }
        return false;
    }

    public static class TestEncoder implements Encoder<Tweet> {
        @Override
        public byte[] toBytes(Tweet tweet) {
            return (tweet.userName + "," + tweet.message).getBytes();
        }
    }

    public static class TestDecoder implements Decoder<Tweet> {
        @Override
        public Tweet fromBytes(byte[] input) {
            String[] tokens = (new String(input)).split(",");
            return new Tweet(tokens[0], tokens[1]);
        }
    }
}