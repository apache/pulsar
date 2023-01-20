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
package org.apache.pulsar.io.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration class for the ElasticSearch Sink Connector.
 */
@Data
@Accessors(chain = true)
public class ElasticSearchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The url of elastic search cluster that the connector connects to"
    )
    private String elasticSearchUrl;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The index name to which the connector writes messages. The default value is the topic name."
                + " It accepts date formats in the name to support event time based index with"
                + " the pattern %{+<date-format>}. For example, suppose the event time of the record"
                + " is 1645182000000L, the indexName is \"logs-%{+yyyy-MM-dd}\", then the formatted"
                + " index name would be \"logs-2022-02-18\"."
    )
    private String indexName;

    @FieldDoc(
        required = false,
        defaultValue = "_doc",
        help = "The type name that the connector writes messages to, with the default value set to _doc."
                + " This value should be set explicitly to a valid type name other than _doc for Elasticsearch version before 6.2,"
                + " and left to the default value otherwise."
    )
    private String typeName = "_doc";

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Sets whether the Sink has to take into account the Schema or if it should simply copy the raw message to Elastichsearch"
    )
    private boolean schemaEnable = false;

    @FieldDoc(
        required = false,
        defaultValue = "1",
        help = "The number of shards of the index"
    )
    private int indexNumberOfShards = 1;

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Create the index if it does not exist"
    )
    private boolean createIndexIfNeeded = false;

    @FieldDoc(
        required = false,
        defaultValue = "0",
        help = "The number of replicas of the index"
    )
    private int indexNumberOfReplicas = 0;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The username used by the connector to connect to the elastic search cluster. If username is set, a password should also be provided."
    )
    private String username;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The password used by the connector to connect to the elastic search cluster. If password is set, a username should also be provided"
    )
    private String password;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The token used by the connector to connect to the ElasticSearch cluster. Only one between basic/token/apiKey authentication mode must be configured."
    )
    private String token;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The apiKey used by the connector to connect to the ElasticSearch cluster. Only one between basic/token/apiKey authentication mode must be configured."
    )
    private String apiKey;

    @FieldDoc(
            required = false,
            defaultValue = "1",
            help = "The maximum number of retries for elasticsearch requests. Use -1 to disable it."
    )
    private int maxRetries = 1;

    @FieldDoc(
            required = false,
            defaultValue = "100",
            help = "The base time in milliseconds to wait when retrying an elasticsearch request."
    )
    private long retryBackoffInMs = 100;

    @FieldDoc(
            required = false,
            defaultValue = "86400",
            help = "The maximum retry time interval in seconds for retrying an elasticsearch request."
    )
    private long maxRetryTimeInSec = 86400;

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Enable the elasticsearch bulk processor to flush write requests based on the number or size of requests, or after a given period."
    )
    private boolean bulkEnabled = false;

    // bulk settings, see https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-bulk.html#java-rest-high-document-bulk-processor
    @FieldDoc(
            required = false,
            defaultValue = "1000",
            help = "The maximum number of actions per elasticsearch bulk request. Use -1 to disable it."
    )
    private int bulkActions = 1000;

    @FieldDoc(
            required = false,
            defaultValue = "5",
            help = "The maximum size in megabytes of elasticsearch bulk requests.Use -1 to disable it."
    )
    private long bulkSizeInMb = 5;

    /**
     * If more than bulkConcurrentRequests are pending, the next bulk request is blocking,
     * meaning the connector.write() is blocking and keeps (bulkConcurrentRequests + 1) * bulkActions
     * records into memory.
     */
    @FieldDoc(
            required = false,
            defaultValue = "0",
            help = "The maximum number of in flight elasticsearch bulk requests. The default 0 allows the execution of a single request. A value of 1 means 1 concurrent request is allowed to be executed while accumulating new bulk requests."
    )
    private int bulkConcurrentRequests = 0;

    @FieldDoc(
            required = false,
            defaultValue = "1000",
            help = "The bulk flush interval flushing any bulk request pending if the interval passes. -1 or zero means the scheduled flushing is disabled."
    )
    private long bulkFlushIntervalInMs = 1000L;

    // connection settings, see https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-config.html
    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Enable elasticsearch request compression."
    )
    private boolean compressionEnabled = false;

    @FieldDoc(
            required = false,
            defaultValue = "5000",
            help = "The elasticsearch client connection timeout in milliseconds."
    )
    private int connectTimeoutInMs = 5000;

    @FieldDoc(
            required = false,
            defaultValue = "1000",
            help = "The time in milliseconds for getting a connection from the elasticsearch connection pool."
    )
    private int connectionRequestTimeoutInMs = 1000;

    @FieldDoc(
            required = false,
            defaultValue = "5",
            help = "Idle connection timeout to prevent a read timeout."
    )
    private int connectionIdleTimeoutInMs = 5;

    @FieldDoc(
            required = false,
            defaultValue = "60000",
            help = "The socket timeout in milliseconds waiting to read the elasticsearch response."
    )
    private int socketTimeoutInMs = 60000;



    @FieldDoc(
            required = false,
            defaultValue = "true",
            help = "Whether to ignore the record key to build the Elasticsearch document _id. If primaryFields is defined, the connector extract the primary fields from the payload to build the document _id. If no primaryFields are provided, elasticsearch auto generates a random document _id."
    )
    private boolean keyIgnore = true;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The comma separated ordered list of field names used to build the Elasticsearch document _id from the record value. If this list is a singleton, the field is converted as a string. If this list has 2 or more fields, the generated _id is a string representation of a JSON array of the field values."
    )
    private String primaryFields = "";

    private ElasticSearchSslConfig ssl = new ElasticSearchSslConfig();

    @FieldDoc(
            required = false,
            defaultValue = "IGNORE",
            help = "How to handle records with null values, possible options are IGNORE, DELETE or FAIL. Default is IGNORE the message."
    )
    private NullValueAction nullValueAction = NullValueAction.IGNORE;

    @FieldDoc(
            required = false,
            defaultValue = "FAIL",
            help = "How to handle elasticsearch rejected documents due to some malformation. Possible options are IGNORE, DELETE or FAIL. Default is FAIL the Elasticsearch document."
    )
    private MalformedDocAction malformedDocAction = MalformedDocAction.FAIL;

    @FieldDoc(
            required = false,
            defaultValue = "true",
            help = "If stripNulls is false, elasticsearch _source includes 'null' for empty fields (for example {\"foo\": null}), otherwise null fields are stripped."
    )
    private boolean stripNulls = true;

    @FieldDoc(
            required = false,
            defaultValue = "AUTO",
            help = "Specify compatibility mode with the ElasticSearch cluster. "
                    + "'AUTO' value will try to auto detect the correct compatibility mode to use. "
                    + "Use 'ELASTICSEARCH_7' if the target cluster is running ElasticSearch 7 or prior. "
                    + "Use 'ELASTICSEARCH' if the target cluster is running ElasticSearch 8 or higher. "
                    + "Use 'OPENSEARCH' if the target cluster is running OpenSearch."
    )
    private CompatibilityMode compatibilityMode = CompatibilityMode.AUTO;

    @FieldDoc(
            defaultValue = "false",
            help = "If canonicalKeyFields is true and record key schema is JSON or AVRO, the serialized object will "
                    + "not consider the properties order."
    )
    private boolean canonicalKeyFields = false;

    @FieldDoc(
            defaultValue = "true",
            help = "If stripNonPrintableCharacters is true, all non-printable characters will be removed from the document."
    )
    private boolean stripNonPrintableCharacters = true;

    @FieldDoc(
            defaultValue = "NONE",
            help = "Hashing algorithm to use for the document id. This is useful in order to be compliant with "
                    + "the ElasticSearch _id hard limit of 512 bytes."
    )
    private IdHashingAlgorithm idHashingAlgorithm = IdHashingAlgorithm.NONE;

    @FieldDoc(
            defaultValue = "false",
            help = "This option only works if idHashingAlgorithm is set."
                    + "If enabled, the hashing is performed only if the id is greater than 512 bytes otherwise "
                    + "the hashing is performed on each document in any case."
    )
    private boolean conditionalIdHashing = false;

    @FieldDoc(
            defaultValue = "false",
            help = "When the message key schema is AVRO or JSON, copy the message key fields into the Elasticsearch _source."
    )
    private boolean copyKeyFields = false;

    public enum MalformedDocAction {
        IGNORE,
        WARN,
        FAIL
    }

    public enum NullValueAction {
        IGNORE,
        DELETE,
        FAIL
    }

    public enum CompatibilityMode {
        AUTO,
        ELASTICSEARCH_7,
        ELASTICSEARCH,
        OPENSEARCH
    }

    public enum IdHashingAlgorithm {
        NONE,
        SHA256,
        SHA512
    }

    public static ElasticSearchConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ElasticSearchConfig.class);
    }

    public static ElasticSearchConfig load(Map<String, Object> map, SinkContext sinkContext) throws IOException {
        return IOConfigUtils.loadWithSecrets(map, ElasticSearchConfig.class, sinkContext);
    }

    public void validate() {
        if (StringUtils.isEmpty(elasticSearchUrl)) {
            throw new IllegalArgumentException("elasticSearchUrl not set.");
        }

        if (StringUtils.isNotEmpty(indexName) && createIndexIfNeeded) {
            // see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
            // date format may contain upper cases, so we need to valid against parsed index name
            IndexNameFormatter.validate(indexName);
            if (indexName.startsWith("-") || indexName.startsWith("_") || indexName.startsWith("+")) {
                throw new IllegalArgumentException("indexName start with an invalid character.");
            }
            if (indexName.equals(".") || indexName.equals("..")) {
                throw new IllegalArgumentException("indexName cannot be . or ..");
            }
            if (indexName.getBytes(StandardCharsets.UTF_8).length > 255) {
                throw new IllegalArgumentException("indexName cannot be longer than 255 bytes.");
            }
        }

        if ((StringUtils.isNotEmpty(username) && StringUtils.isEmpty(password))
                || (StringUtils.isEmpty(username) && StringUtils.isNotEmpty(password))) {
            throw new IllegalArgumentException("Values for both Username & password are required.");
        }

        boolean basicAuthSet = StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password);
        boolean tokenAuthSet = StringUtils.isNotEmpty(token);
        boolean apiKeySet = StringUtils.isNotEmpty(apiKey);
        if ((basicAuthSet && tokenAuthSet && apiKeySet)
                || (basicAuthSet && tokenAuthSet)
                || (basicAuthSet && apiKeySet)
                || (tokenAuthSet && apiKeySet)) {
            throw new IllegalArgumentException("Only one between basic/token/apiKey authentication mode must be configured.");
        }

        if (indexNumberOfShards <= 0) {
            throw new IllegalArgumentException("indexNumberOfShards must be a strictly positive integer.");
        }

        if (indexNumberOfReplicas < 0) {
            throw new IllegalArgumentException("indexNumberOfReplicas must be a positive integer.");
        }

        if (connectTimeoutInMs < 0) {
            throw new IllegalArgumentException("connectTimeoutInMs must be a positive integer.");
        }

        if (connectionRequestTimeoutInMs < 0) {
            throw new IllegalArgumentException("connectionRequestTimeoutInMs must be a positive integer.");
        }

        if (socketTimeoutInMs < 0) {
            throw new IllegalArgumentException("socketTimeoutInMs must be a positive integer.");
        }

        if (bulkConcurrentRequests < 0) {
            throw new IllegalArgumentException("bulkConcurrentRequests must be a positive integer.");
        }
    }
}
