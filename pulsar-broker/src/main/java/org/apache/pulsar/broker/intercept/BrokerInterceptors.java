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
package org.apache.pulsar.broker.intercept;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.TopicListingResult;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * A collection of broker interceptor.
 */
@Slf4j
public class BrokerInterceptors implements BrokerInterceptor {

    private final Map<String, BrokerInterceptorWithClassLoader> interceptors;

    public BrokerInterceptors(Map<String, BrokerInterceptorWithClassLoader> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * Load the broker event interceptor for the given <tt>interceptor</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of broker event interceptor
     */
    public static BrokerInterceptor load(ServiceConfiguration conf) throws IOException {
        if (conf.getBrokerInterceptors().isEmpty()) {
            return null;
        }
        BrokerInterceptorDefinitions definitions =
                BrokerInterceptorUtils.searchForInterceptors(conf.getBrokerInterceptorsDirectory(),
                        conf.getNarExtractionDirectory());

        // Use LinkedHashMap as a temporary container to ensure insertion order
        Map<String, BrokerInterceptorWithClassLoader> orderedInterceptorMap = new LinkedHashMap<>();

        conf.getBrokerInterceptors().forEach(interceptorName -> {

            BrokerInterceptorMetadata definition = definitions.interceptors().get(interceptorName);
            if (null == definition) {
                throw new RuntimeException("No broker interceptor is found for name `" + interceptorName
                        + "`. Available broker interceptors are : " + definitions.interceptors());
            }

            BrokerInterceptorWithClassLoader interceptor;
            try {
                interceptor = BrokerInterceptorUtils.load(definition, conf.getNarExtractionDirectory());
                if (interceptor != null) {
                    orderedInterceptorMap.put(interceptorName, interceptor);
                }
                log.info("Successfully loaded broker interceptor for name `{}`", interceptorName);
            } catch (IOException e) {
                log.error("Failed to load the broker interceptor for name `" + interceptorName + "`", e);
                throw new RuntimeException("Failed to load the broker interceptor for name `" + interceptorName + "`");
            }
        });

        if (!orderedInterceptorMap.isEmpty()) {
            return new BrokerInterceptors(Map.copyOf(orderedInterceptorMap));
        } else {
            return null;
        }
    }

    @Override
    public void onMessagePublish(Producer producer,
                                 ByteBuf headersAndPayload,
                                 Topic.PublishContext publishContext) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.onMessagePublish(producer, headersAndPayload, publishContext);
            }
        }
    }

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.beforeSendMessage(subscription, entry, ackSet, msgMetadata);
            }
        }
    }

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata,
                                  Consumer consumer) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.beforeSendMessage(subscription, entry, ackSet, msgMetadata, consumer);
            }
        }
    }

    @Override
    public void consumerCreated(ServerCnx cnx,
                                 Consumer consumer,
                                 Map<String, String> metadata) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.consumerCreated(
                        cnx,
                        consumer,
                        metadata);
            }
        }
    }

    @Override
    public void consumerClosed(ServerCnx cnx,
                               Consumer consumer,
                               Map<String, String> metadata) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.consumerClosed(cnx, consumer, metadata);
            }
        }
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer,
                                 Map<String, String> metadata){
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.producerCreated(cnx, producer, metadata);
            }
        }
    }

    @Override
    public void producerClosed(ServerCnx cnx,
                               Producer producer,
                               Map<String, String> metadata) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.producerClosed(cnx, producer, metadata);
            }
        }
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                 long entryId, Topic.PublishContext publishContext) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.messageProduced(cnx, producer, startTimeNs, ledgerId, entryId, publishContext);
            }
        }
    }

    @Override
    public  void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.messageDispatched(cnx, consumer, ledgerId, entryId, headersAndPayload);
            }
        }
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer,
                              CommandAck ackCmd) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.messageAcked(cnx, consumer, ackCmd);
            }
        }
    }

    @Override
    public void txnOpened(long tcId, String txnID) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.txnOpened(tcId, txnID);
            }
        }
    }

    @Override
    public void txnEnded(String txnID, long txnAction) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.txnEnded(txnID, txnAction);
            }
        }
    }


    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        if (interceptorsEnabled()) {
            for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
                value.onConnectionCreated(cnx);
            }
        }
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
            value.onPulsarCommand(command, cnx);
        }
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
            value.onConnectionClosed(cnx);
        }
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
        for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
            value.onWebserviceRequest(request);
        }
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response)
            throws IOException, ServletException {
        for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
            value.onWebserviceResponse(request, response);
        }
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        for (BrokerInterceptorWithClassLoader v : interceptors.values()) {
            v.initialize(pulsarService);
        }
    }

   @Override
   public CompletableFuture<Optional<TopicListingResult>> interceptGetTopicsOfNamespace(
       NamespaceName namespace,
       CommandGetTopicsOfNamespace.Mode mode,
       Optional<String> topicsPattern,
       Map<String, String> properties) {

       if (!interceptorsEnabled()) {
           // No interceptors configured, return PassThrough immediately
           return CompletableFuture.completedFuture(Optional.empty());
       }

       CompletableFuture<Optional<TopicListingResult>> resultFuture = new CompletableFuture<>();

       List<BrokerInterceptor> interceptorList = new ArrayList<>(interceptors.values());

       // Start recursive processing
       interceptRemainingGetTopicsOfNamespace(resultFuture, interceptorList, namespace, mode, topicsPattern,
           properties, 0);

       return resultFuture;
   }

    private void interceptRemainingGetTopicsOfNamespace(
        CompletableFuture<Optional<TopicListingResult>> resultFuture,
        List<BrokerInterceptor> list,
        NamespaceName namespace,
        CommandGetTopicsOfNamespace.Mode mode,
        Optional<String> topicsPattern,
        Map<String, String> properties,
        int index) {
        // Return PassThrough if no more interceptors to process
        if (index >= list.size()) {
            resultFuture.complete(Optional.empty());
            return;
        }

        BrokerInterceptor interceptor = list.get(index);

        try {
            CompletableFuture<Optional<TopicListingResult>> future = interceptor
                .interceptGetTopicsOfNamespace(namespace, mode, topicsPattern, properties);

            // Prevent the plugin from directly returning null, skip to the next interceptor
            if (future == null) {
                interceptRemainingGetTopicsOfNamespace(resultFuture, list, namespace, mode, topicsPattern,
                    properties, index + 1);
                return;
            }

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    // Ignore the error and try the next one
                    log.warn("Interceptor {} failed for namespace {}: {}", interceptor.getClass().getName(),
                            namespace, ex.getMessage());
                    interceptRemainingGetTopicsOfNamespace(resultFuture, list, namespace, mode, topicsPattern,
                        properties, index + 1);
                } else if (result != null && result.isPresent()) {
                    // Interception successful (Found match) -> Complete Future and end recursion
                    resultFuture.complete(result);
                } else {
                    // Interceptor selects PassThrough (result == null || result.isPassThrough) -> try the next one
                    interceptRemainingGetTopicsOfNamespace(resultFuture, list, namespace, mode, topicsPattern,
                        properties, index + 1);
                }
            });

        } catch (Throwable t) {
            // Ignore the error and try the next one
            log.warn("Interceptor {} failed synchronously for namespace {}", interceptor.getClass().getName(),
                namespace, t);
            interceptRemainingGetTopicsOfNamespace(resultFuture, list, namespace, mode, topicsPattern, properties,
                index + 1);
        }
    }

    @Override
    public void close() {
        interceptors.values().forEach(BrokerInterceptorWithClassLoader::close);
    }

    private boolean interceptorsEnabled() {
        return interceptors != null && !interceptors.isEmpty();
    }

    @VisibleForTesting
    public Map<String, BrokerInterceptorWithClassLoader> getInterceptors() {
        return interceptors;
    }
}
