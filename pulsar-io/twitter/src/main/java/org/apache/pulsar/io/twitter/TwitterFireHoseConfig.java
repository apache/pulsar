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
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import com.twitter.hbc.core.Constants;
import lombok.*;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class TwitterFireHoseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String tokenSecret;
    // Most firehose events have null createdAt time. If this parameter is set to true
    // then we estimate the createdTime of each firehose event to be current time.
    private Boolean guestimateTweetTime = false;

    // ------ Optional property keys

    private String clientName = "openconnector-twitter-source";
    private String clientHosts = Constants.STREAM_HOST;
    private int clientBufferSize = 50000;

    public static TwitterFireHoseConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), TwitterFireHoseConfig.class);
    }

    public static TwitterFireHoseConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), TwitterFireHoseConfig.class);
    }
}