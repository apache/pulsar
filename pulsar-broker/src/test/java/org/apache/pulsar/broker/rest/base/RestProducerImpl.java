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
package org.apache.pulsar.broker.rest.base;

import org.apache.pulsar.broker.rest.base.api.RestProducer;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.BaseResource;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.websocket.data.ProducerMessages;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

public class RestProducerImpl extends BaseResource implements RestProducer {

    private final WebTarget adminV2;

    public RestProducerImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.adminV2 = web.path("/topics");
    }


    @Override
    public void send(String topic, ProducerMessages producerMessages) throws PulsarAdminException {
        TopicName tn = TopicName.get(topic);
        WebTarget path = targetPath(tn);
        sync(() -> asyncPostRequest(path, Entity.entity(producerMessages, MediaType.APPLICATION_JSON)));
    }

    private WebTarget targetPath(TopicName topic, String... parts) {
        final WebTarget base = adminV2;
        WebTarget topicPath = base.path(topic.getRestPath());
        topicPath = addParts(topicPath, parts);
        return topicPath;
    }
}
