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

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.intercept.MessagePayloadProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerPayloadProcessor implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerPayloadProcessor.class);
    private static final String INDEX = "index";
    private ServiceConfiguration serviceConfig;


    private final MessagePayloadProcessor brokerEntryPayloadProcessor;
    public ManagedLedgerPayloadProcessor(ServiceConfiguration serviceConfig,
                                         MessagePayloadProcessor brokerEntryPayloadProcessor) {
        this.serviceConfig = serviceConfig;
        this.brokerEntryPayloadProcessor = brokerEntryPayloadProcessor;
    }
    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
        return null;
    }
    @Override
    public ByteBuf beforeStoreEntryToLedger(OpAddEntry op, ByteBuf ledgerData) {
        Map<String, Object> contextMap = new HashMap<>();

        ByteBuf newData = brokerEntryPayloadProcessor.interceptIn(
                serviceConfig.getClusterName(),
                ledgerData, contextMap);
        if (op.getCtx() instanceof Topic.PublishContext){
            Topic.PublishContext ctx = (Topic.PublishContext) op.getCtx();
            contextMap.forEach((k, v) -> {
                ctx.setProperty(k, v);
            });
        }
        return newData;
    }

    @Override
    public ByteBuf beforeCacheEntryFromLedger(ByteBuf ledgerData){
        return brokerEntryPayloadProcessor.interceptOut(ledgerData);
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
    }
    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LedgerHandle lh) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        promise.complete(null);
        return promise;
    }
    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
    }
}
