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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.twitter.hbc.core.Constants;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration object for the Twitter Firehose Connector.
 */
@Data
@Accessors(chain = true)
public class TwitterFireHoseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Your twitter app consumer key. See "
                + "https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens for details"
    )
    private String consumerKey;

    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Your twitter app consumer secret. "
                + "See https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens for details"
    )
    private String consumerSecret;

    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Your twitter app token. "
                + "See https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens for details"
    )
    private String token;

    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Your twitter app token secret. "
                + "See https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens for details"
    )
    private String tokenSecret;

    // Most firehose events have null createdAt time. If this parameter is set to true
    // then we estimate the createdTime of each firehose event to be current time.
    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Most firehose events have null createdAt time.If this parameter is set to true, "
                + "the connector estimates the createdTime of each firehose event to be current time."
    )
    private Boolean guestimateTweetTime = false;

    // ------ Optional property keys

    @FieldDoc(
        required = false,
        defaultValue = "pulsario-twitter-source",
        help = "The Twitter Firehose Client name"
    )
    private String clientName = "pulsario-twitter-source";

    @FieldDoc(
        required = false,
        defaultValue = Constants.STREAM_HOST,
        help = "The Twitter Firehose stream hosts that the connector connects to"
    )
    private String clientHosts = Constants.STREAM_HOST;

    @FieldDoc(
        required = false,
        defaultValue = "50000",
        help = "The Twitter Firehose client buffer size"
    )
    private int clientBufferSize = 50000;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "A comma separated list of user IDs, indicating the users to return statuses for in the stream."
    )
    private String followings;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Keywords to track. Phrases of keywords are specified by a comma-separated list."
    )
    private String terms;

    public static TwitterFireHoseConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), TwitterFireHoseConfig.class);
    }

    public static TwitterFireHoseConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), TwitterFireHoseConfig.class);
    }

    public void validate() throws IllegalArgumentException {
        if (getConsumerKey() == null || getConsumerSecret() == null
             || getToken() == null || getTokenSecret() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }
    }

    public List<Long> getFollowings() {
        if (StringUtils.isBlank(followings)) {
            return Collections.emptyList();
        }

        List<Long> result = new ArrayList<Long> ();

        for (String s: StringUtils.split(followings, ",")) {
            try {
                result.add(Long.parseLong(StringUtils.trim(s)));
            } catch (NumberFormatException nfEx) {
                // Ignore these
            }
        }

        return CollectionUtils.isEmpty(result) ? Collections.emptyList() : result;
    }

    public List<String> getTrackTerms() {
        if (StringUtils.isBlank(terms)) {
            return Collections.emptyList();
        }

        return Lists.newArrayList(StringUtils.split(terms, ","));
    }
}