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
package org.apache.pulsar.io.redis.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.pulsar.io.redis.RedisAbstractConfig;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class RedisSinkConfig extends RedisAbstractConfig implements Serializable {

    private static final long serialVersionUID = 4686456460365805717L;

    @FieldDoc(
        required = false,
        defaultValue = "10000L",
        help = "The amount of time in milliseconds before an operation is marked as timed out")
    private long operationTimeout = 10000L;

    @FieldDoc(
        required = false,
        defaultValue = "1000L",
        help = "The Redis operation time in milliseconds")
    private long batchTimeMs = 1000L;

    @FieldDoc(
        required = false,
        defaultValue = "200",
        help = "The batch size of write to Redis database"
    )
    private int batchSize = 200;

    public static RedisSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), RedisSinkConfig.class);
    }

    public static RedisSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), RedisSinkConfig.class);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(operationTimeout > 0, "operationTimeout must be a positive long.");
        Preconditions.checkArgument(batchTimeMs > 0, "batchTimeMs must be a positive long.");
        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive integer.");
    }
}
