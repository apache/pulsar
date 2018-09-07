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
package org.apache.pulsar.io.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration class for the ElasticSearch Sink Connector.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class ElasticSearchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String elasticSearchUrl;

    private String indexName;

    private int indexNumberOfShards = 1;

    private int indexNumberOfReplicas = 1;

    private String username;

    private String password;

    public static ElasticSearchConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ElasticSearchConfig.class);
    }

    public static ElasticSearchConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), ElasticSearchConfig.class);
    }

    public void validate() {
        if (StringUtils.isEmpty(elasticSearchUrl) || StringUtils.isEmpty(indexName)) {
            throw new IllegalArgumentException("Required property not set.");
        }

        if ((StringUtils.isNotEmpty(username) && StringUtils.isEmpty(password))
           || (StringUtils.isEmpty(username) && StringUtils.isNotEmpty(password))) {
            throw new IllegalArgumentException("Values for both Username & password are required.");
        }

        if (indexNumberOfShards < 1) {
            throw new IllegalArgumentException("indexNumberOfShards must be a positive integer");
        }

        if (indexNumberOfReplicas < 1) {
            throw new IllegalArgumentException("indexNumberOfReplicas must be a positive integer");
        }
    }
}