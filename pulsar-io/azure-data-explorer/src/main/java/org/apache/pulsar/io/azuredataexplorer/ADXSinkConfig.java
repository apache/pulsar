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
package org.apache.pulsar.io.azuredataexplorer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;


@Data
@Accessors(chain = true)
public class ADXSinkConfig implements Serializable {

    @FieldDoc(required = true, defaultValue = "", help = "The ADX cluster URL")
    private String clusterUrl;

    @FieldDoc(required = true, defaultValue = "", help = "The database name to which data need to be ingested")
    private String database;

    @FieldDoc(required = true, defaultValue = "", help = "Table name to which pulsar data need to be ingested.")
    private String table;

    @FieldDoc(defaultValue = "", help = "The AAD app Id for authentication", sensitive = true)
    private String appId;

    @FieldDoc(defaultValue = "", help = "The AAD app secret for authentication", sensitive = true)
    private String appKey;

    @FieldDoc(defaultValue = "", help = "The tenant Id for authentication")
    private String tenantId;

    @FieldDoc(defaultValue = "", help = "The Managed Identity credential for authentication."
            + " Set this with clientId in case of User assigned MI."
            + " and 'system' in case of System assigned managed identity")
    private String managedIdentityId;

    @FieldDoc(defaultValue = "", help = "The mapping reference for ingestion")
    private String mappingRefName;

    @FieldDoc(defaultValue = "", help = "The type of mapping reference provided")
    private String mappingRefType;

    @FieldDoc(defaultValue = "false", help = "Denotes if flush should happen immediately without aggregation. "
            + "Not recommended to enable flushImmediately for production workloads")
    private boolean flushImmediately = false;

    @FieldDoc(defaultValue = "100", help = "For batching, this defines the number of "
            + "records to hold for batching, to sink data to adx")
    private int batchSize = 100;

    @FieldDoc(defaultValue = "10000", help = "For batching, this defines the time to hold"
            + " records before sink to adx")
    private long batchTimeMs = 10000;

    @FieldDoc(defaultValue = "1", help = "Max retry attempts, In case of transient ingestion errors")
    private int maxRetryAttempts = 1;

    @FieldDoc(defaultValue = "10", help = "Period of time in milliseconds to backoff"
            + " before retry for transient errors")
    private long retryBackOffTime = 10;


    public static ADXSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ADXSinkConfig.class);
    }

    protected static ADXSinkConfig load(Map<String, Object> config, SinkContext sinkContext) {
        return IOConfigUtils.loadWithSecrets(config, ADXSinkConfig.class, sinkContext);
    }

    public void validate() throws Exception {
        Objects.requireNonNull(clusterUrl, "clusterUrl property not set.");
        Objects.requireNonNull(database, "database property not set.");
        Objects.requireNonNull(table, "table property not set.");
        if (managedIdentityId == null && (appId == null || appKey == null || tenantId == null)) {
            throw new Exception("Auth credentials not valid");
        }
    }
}
