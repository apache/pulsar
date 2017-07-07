/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.partition.PartitionedTopicMetadata;

class HttpLookupService implements LookupService {

    private final HttpClient httpClient;
    private final boolean useTls;
    private static final String BasePath = "lookup/v2/destination/";

	public HttpLookupService(HttpClient httpClient, boolean useTls) {
		this.httpClient = httpClient;
		this.useTls = useTls;
	}

    /**
     * Calls http-lookup api to find broker-service address which can serve a given topic. 
     * 
     * @param destination: topic-name
     * @return broker-socket-address that serves given topic 
     */
    @SuppressWarnings("deprecation")
    public CompletableFuture<InetSocketAddress> getBroker(DestinationName destination) {
        return httpClient.get(BasePath + destination.getLookupName(), LookupData.class).thenCompose(lookupData -> {
            // Convert LookupData into as SocketAddress, handling exceptions
        	URI uri = null;
            try {
                if (useTls) {
                    uri = new URI(lookupData.getBrokerUrlTls());
                } else {
                    String serviceUrl = lookupData.getBrokerUrl();
                    if (serviceUrl == null) {
                        serviceUrl = lookupData.getNativeUrl();
                    }
                    uri = new URI(serviceUrl);
                }
                return CompletableFuture.completedFuture(new InetSocketAddress(uri.getHost(), uri.getPort()));
            } catch (Exception e) {
                // Failed to parse url
            	log.warn("[{}] Lookup Failed due to invalid url {}, {}", destination, uri, e.getMessage());
                return FutureUtil.failedFuture(e);
            }
        });
    }
    
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(DestinationName destination) {
    	return httpClient.get(String.format("admin/%s/partitions", destination.getLookupName()),
                PartitionedTopicMetadata.class);
    }
    
    public String getServiceUrl() {
    	return httpClient.url.toString();
    }
    
    private static final Logger log = LoggerFactory.getLogger(HttpLookupService.class);
}
