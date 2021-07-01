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
package org.apache.pulsar.broker.intercept;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.eclipse.jetty.server.Response;

@Slf4j
public class CounterBrokerInterceptor implements BrokerInterceptor {

    int beforeSendCount = 0;
    int count = 0;
    private List<ResponseEvent> responseList = new ArrayList<>();

    @Data
    @AllArgsConstructor
    public class ResponseEvent {
        private String requestUri;
        private int responseStatus;
    }

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata) {
        log.info("Send message to topic {}, subscription {}",
            subscription.getTopic(), subscription.getName());
        beforeSendCount++;
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        log.info("[{}] On [{}] Pulsar command", count, command.getType().name());
        count ++;
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        // np-op
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) {
        count ++;
        log.info("[{}] On [{}] Webservice request", count, ((HttpServletRequest)request).getRequestURL().toString());
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
        count ++;
        log.info("[{}] On [{}] Webservice response {}", count, ((HttpServletRequest)request).getRequestURL().toString(), response);
        if (response instanceof Response) {
            Response res = (Response) response;
            responseList.add(new ResponseEvent(res.getHttpChannel().getRequest().getRequestURI(), res.getStatus()));
        }
    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        count = 100;
        chain.doFilter(request, response);
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {

    }

    @Override
    public void close() {

    }

    public int getCount() {
        return count;
    }

    public int getBeforeSendCount() {
        return beforeSendCount;
    }

    public void clearResponseList() {
        responseList.clear();
    }

    public List<ResponseEvent> getResponseList() {
        return responseList;
    }
}
