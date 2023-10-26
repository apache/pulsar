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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class ADXSinkConfig implements Serializable {

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The ADX cluser URL")
    private String clusterUrl;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The database name to which data need to be ingested")
    private String database;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Table name to which pulsar data need to be ingested.")
    private String table;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The AAD app Id for authentication")
    private String appId;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The AAD app secret for authentication")
    private String appKey;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The tenent Id for authentication")
    private String tenantId;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The Managed Identity credential for authentication. Set this with clientId in case of User assigned MI.")
    private String managedIdentityId;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The mapping reference for ingestion")
    private String mappingRefName;

    @FieldDoc(
            required = false,
            defaultValue = "CSV",
            help = "The type of mapping reference provided")
    private String mappingRefType;//="CSV";

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "")
    private boolean flushImmediately=false;

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "This defines the ingestion type managed Ingestion or queued Ingestion")
    private boolean managedIngestion=false;

    @FieldDoc(
            required = false,
            defaultValue = "100",
            help = "For batching, this defines the number of records to hold for batching, to sink data to adx")
    private int batchSize=100;

    @FieldDoc(
            required = false,
            defaultValue = "10000",
            help = "For batching, this defines the time to hold records before sink to adx")
    private long batchTimeMs=10000;

    public static ADXSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ADXSinkConfig.class);
    }
    protected static ADXSinkConfig load(Map<String, Object> config) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(config), ADXSinkConfig.class);
    }


}
