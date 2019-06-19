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
package org.apache.pulsar.client.admin.internal;

import javax.ws.rs.client.WebTarget;

import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;

public class LookupImpl extends BaseResource implements Lookup {

    private final WebTarget v2lookup;
    private final boolean useTls;

    public LookupImpl(WebTarget web, Authentication auth, boolean useTls, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.useTls = useTls;
        v2lookup = web.path("/lookup/v2");
    }

    @Override
    public String lookupTopic(String topic) throws PulsarAdminException {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget target = v2lookup.path(prefix).path(topicName.getLookupName());

        try {
            return doTopicLookup(target);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getBundleRange(String topic) throws PulsarAdminException {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget target = v2lookup.path(prefix).path(topicName.getLookupName()).path("bundle");

        try {
            return request(target).get(String.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private String doTopicLookup(WebTarget lookupResource) throws PulsarAdminException {
        LookupData lookupData = request(lookupResource).get(LookupData.class);
        if (useTls) {
            return lookupData.getBrokerUrlTls();
        } else {
            return lookupData.getBrokerUrl();
        }
    }

}
