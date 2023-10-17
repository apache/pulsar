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
package org.apache.pulsar.websocket;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Splitter;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;

/**
 * Subscribing for multi-topic.
 */
public class MultiTopicConsumerHandler extends ConsumerHandler {

    public MultiTopicConsumerHandler(WebSocketService service, HttpServletRequest request,
                                     ServletUpgradeResponse response) {
        super(service, request, response);
    }

    @Override
    protected void extractTopicName(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);

        // V3 Format must be like :
        // /ws/v3/consumer/my-subscription?topicsPattern="a.*"  //ws/v3/consumer/my-subscription?topics="a,b,c"
        checkArgument(parts.size() >= 4, "Invalid topic name format");
        checkArgument(parts.get(2).equals("v3"));
        checkArgument(queryParams.containsKey("topicsPattern") || queryParams.containsKey("topics"),
                "Should set topics or topicsPattern");
        checkArgument(!(queryParams.containsKey("topicsPattern") && queryParams.containsKey("topics")),
                "Topics must be null when use topicsPattern");
        topicsPattern = queryParams.get("topicsPattern");
        topics = queryParams.get("topics");
        if (topicsPattern != null) {
            topic = TopicName.get(topicsPattern);
        } else {
            // Multi topics only use the first topic name，
            topic = TopicName.get(Splitter.on(",").splitToList(topics).get(0));
        }
    }

    @Override
    public String extractSubscription(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);
        // v3 Format must be like :
        // /ws/v3/consumer/my-subscription?topicsPattern="a.*"  //ws/v3/consumer/my-subscription?topics="a,b,c"
        checkArgument(parts.size() >= 5 , "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));
        checkArgument(parts.get(2).equals("v3"));
        checkArgument(parts.get(4).length() > 0, "Empty subscription name");

        return Codec.decode(parts.get(4));
    }
}
