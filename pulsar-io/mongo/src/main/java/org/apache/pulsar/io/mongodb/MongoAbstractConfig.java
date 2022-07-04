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
package org.apache.pulsar.io.mongodb;

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration object for all MongoDB components.
 */
@Data
@Accessors(chain = true)
public abstract class MongoAbstractConfig implements Serializable {

    private static final long serialVersionUID = -3830568531897300005L;

    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final long DEFAULT_BATCH_TIME_MS = 1000;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The URI of MongoDB that the connector connects to "
                    + "(see: https://docs.mongodb.com/manual/reference/connection-string/)"
    )
    private final String mongoUri;

    @FieldDoc(
            defaultValue = "",
            help = "The database name to which the collection belongs "
                    + "and which must be watched for the source connector "
                    + "(required for the sink connector)"
    )
    private final String database;

    @FieldDoc(
            defaultValue = "",
            help = "The collection name where the messages are written "
                    + "or which is watched for the source connector "
                    + "(required for the sink connector)"
    )
    private final String collection;

    @FieldDoc(
            defaultValue = "" + DEFAULT_BATCH_SIZE,
            help = "The batch size of write to or read from the database"
    )
    private final int batchSize;

    @FieldDoc(
            defaultValue = "" + DEFAULT_BATCH_TIME_MS,
            help = "The batch operation interval in milliseconds")
    private final long batchTimeMs;

    public MongoAbstractConfig() {
        this(null, null, null, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_TIME_MS);
    }

    @JsonCreator
    public MongoAbstractConfig(
            @JsonProperty("mongoUri") String mongoUri,
            @JsonProperty("database") String database,
            @JsonProperty("collection") String collection,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeMs") long batchTimeMs
    ) {
        this.mongoUri = mongoUri;
        this.database = database;
        this.collection = collection;
        this.batchSize = batchSize;
        this.batchTimeMs = batchTimeMs;
    }

    public void validate() {
        checkArgument(!StringUtils.isEmpty(getMongoUri()), "Required MongoDB URI is not set.");
        checkArgument(getBatchSize() > 0, "batchSize must be a positive integer.");
        checkArgument(getBatchTimeMs() > 0, "batchTimeMs must be a positive long.");
    }
}
