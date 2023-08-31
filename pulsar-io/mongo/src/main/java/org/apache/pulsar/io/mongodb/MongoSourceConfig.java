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
package org.apache.pulsar.io.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration class for the MongoDB Source Connectors.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class MongoSourceConfig extends MongoAbstractConfig {

    private static final long serialVersionUID = 1152890092264945317L;

    public static final SyncType DEFAULT_SYNC_TYPE = SyncType.INCR_SYNC;

    public static final String DEFAULT_SYNC_TYPE_STR = "INCR_SYNC";

    @FieldDoc(
            defaultValue = DEFAULT_SYNC_TYPE_STR,
            help = "The message synchronization type of the source connector. "
                    + "The field values can be of two types: incr and full. "
                    + "When it is set to incr, the source connector will only watch for changes made from now on. "
                    + "When it is set to full, the source connector will synchronize currently existing messages "
                    + "and watch for future changes."
    )
    private SyncType syncType = DEFAULT_SYNC_TYPE;

    @JsonCreator
    public MongoSourceConfig(
            @JsonProperty("mongoUri") String mongoUri,
            @JsonProperty("database") String database,
            @JsonProperty("collection") String collection,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeMs") long batchTimeMs,
            @JsonProperty("syncType") String syncType
    ) {
        super(mongoUri, database, collection, batchSize, batchTimeMs);
        setSyncType(syncType);
    }

    public static MongoSourceConfig load(String yamlFile) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final MongoSourceConfig cfg = mapper.readValue(new File(yamlFile), MongoSourceConfig.class);

        return cfg;
    }

    public static MongoSourceConfig load(Map<String, Object> map) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final MongoSourceConfig cfg =
                mapper.readValue(mapper.writeValueAsString(map), MongoSourceConfig.class);

        return cfg;
    }

    /**
     * @param syncTypeStr Sync type string.
     */
    private void setSyncType(String syncTypeStr) {
        // if syncType is not set, the default sync type is used
        if (StringUtils.isEmpty(syncTypeStr)) {
            this.syncType = DEFAULT_SYNC_TYPE;
            return;
        }

        // if syncType is set but not correct, an exception will be thrown
        try {
            this.syncType = SyncType.valueOf(syncTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The value of the syncType field is incorrect.");
        }
    }

    @Override
    public void validate() {
        super.validate();
    }
}
