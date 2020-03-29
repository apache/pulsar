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

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A simple abstract class for Solr sink
 */
@Slf4j
public abstract class SolrAbstractSink<T> implements Sink<T> {

    private SolrSinkConfig solrSinkConfig;
    private SolrClient client;
    private boolean enableBasicAuth;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        solrSinkConfig = SolrSinkConfig.load(config);
        solrSinkConfig.validate();

        enableBasicAuth = !Strings.isNullOrEmpty(solrSinkConfig.getUsername());

        SolrMode solrMode;
        try {
            solrMode = SolrMode.valueOf(solrSinkConfig.getSolrMode().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal Solr mode, valid values are: "
                + Arrays.asList(SolrMode.values()));
        }

        client = getClient(solrMode, solrSinkConfig.getSolrUrl());
    }

    @Override
    public void write(Record<T> record) {
        UpdateRequest updateRequest = new UpdateRequest();
        if (solrSinkConfig.getSolrCommitWithinMs() > 0) {
            updateRequest.setCommitWithin(solrSinkConfig.getSolrCommitWithinMs());
        }
        if (enableBasicAuth) {
            updateRequest.setBasicAuthCredentials(
                solrSinkConfig.getUsername(),
                solrSinkConfig.getPassword()
            );
        }

        SolrInputDocument document = convert(record);
        updateRequest.add(document);

        try {
            UpdateResponse updateResponse = updateRequest.process(client, solrSinkConfig.getSolrCollection());
            if (updateResponse.getStatus() == 0) {
                record.ack();
            } else {
                record.fail();
            }
        } catch (SolrServerException | IOException e) {
            record.fail();
            log.warn("Solr update document exception ", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    // convert record as a Solr document
    public abstract SolrInputDocument convert(Record<T> message);

    public static SolrClient getClient(SolrMode solrMode, String url) {
        SolrClient solrClient = null;
        if (solrMode.equals(SolrMode.STANDALONE)) {
            HttpSolrClient.Builder builder = new HttpSolrClient.Builder(url);
            solrClient = builder.build();
        }
        if (solrMode.equals(SolrMode.SOLRCLOUD)) {
            int chrootIndex = url.indexOf("/");
            Optional<String> chroot = Optional.empty();
            if (chrootIndex > 0) {
                chroot = Optional.of(url.substring(chrootIndex));
            }
            String zkUrls = chrootIndex > 0 ? url.substring(0, chrootIndex) : url;
            List<String> zkHosts = Arrays.asList(zkUrls.split(","));
            CloudSolrClient.Builder builder = new CloudSolrClient.Builder(zkHosts, chroot);
            solrClient = builder.build();
        }
        return solrClient;
    }

    public enum SolrMode {
        STANDALONE,
        SOLRCLOUD
    }
}
