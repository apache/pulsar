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
package org.apache.pulsar.io.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Configuration class for the Solr Sink Connector.
 */
@Data
@Accessors(chain = true)
public class SolrSinkConfig implements Serializable {

    private static final long serialVersionUID = -4849066206354610110L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Comma separated zookeeper hosts with chroot used in SolrCloud mode (eg: localhost:2181,localhost:2182/chroot)"
            + " or Url to connect to solr used in Standalone mode (e.g. localhost:8983/solr)"
    )
    private String solrUrl;

    @FieldDoc(
        required = true,
        defaultValue = "SolrCloud",
        help = "The client mode to use when interacting with the Solr cluster. Possible values [Standalone, SolrCloud]")
    private String solrMode = "SolrCloud";

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Solr collection name to which records need to be written")
    private String solrCollection;

    @FieldDoc(
        required = false,
        defaultValue = "10",
        help = "Commit within milli seconds for solr update, if none passes defaults to 10 ms")
    private int solrCommitWithinMs = 10;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The username to use for basic authentication")
    private String username;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The password to use for basic authentication")
    private String password;

    public static SolrSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), SolrSinkConfig.class);
    }

    public static SolrSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), SolrSinkConfig.class);
    }

    public void validate() {
        Preconditions.checkNotNull(solrUrl, "solrUrl property not set.");
        Preconditions.checkNotNull(solrMode, "solrMode property not set.");
        Preconditions.checkNotNull(solrCollection, "solrCollection property not set.");
        Preconditions.checkArgument(solrCommitWithinMs > 0, "solrCommitWithinMs must be a positive integer.");
    }
}
