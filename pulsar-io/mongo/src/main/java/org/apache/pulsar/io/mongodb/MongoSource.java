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
package org.apache.pulsar.io.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The base class for MongoDB sources.
 */
@Connector(
        name = "mongo",
        type = IOType.SOURCE,
        help = "A source connector that sends mongodb documents to pulsar",
        configClass = MongoConfig.class
)
@Slf4j
public class MongoSource extends PushSource<byte[]> {

    private final Supplier<MongoClient> clientProvider;

    private MongoConfig mongoConfig;

    private MongoClient mongoClient;

    private Thread streamThread;

    private ChangeStreamPublisher<Document> stream;


    public MongoSource() {
        this(null);
    }

    public MongoSource(Supplier<MongoClient> clientProvider) {
        this.clientProvider = clientProvider;
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        log.info("Open MongoDB Source");

        mongoConfig = MongoConfig.load(config);
        mongoConfig.validate(false, false);

        if (clientProvider != null) {
            mongoClient = clientProvider.get();
        } else {
            mongoClient = MongoClients.create(mongoConfig.getMongoUri());
        }

        if (StringUtils.isEmpty(mongoConfig.getDatabase())) {
            // Watch all databases
            log.info("Watch all");
            stream = mongoClient.watch();

        } else {
            final MongoDatabase db = mongoClient.getDatabase(mongoConfig.getDatabase());

            if (StringUtils.isEmpty(mongoConfig.getCollection())) {
                // Watch all collections in a database
                log.info("Watch db: {}", db.getName());
                stream = db.watch();

            } else {
                // Watch a collection

                final MongoCollection<Document> collection = db.getCollection(mongoConfig.getCollection());
                log.info("Watch collection: {} {}", db.getName(), mongoConfig.getCollection());
                stream = collection.watch();
            }
        }

        stream.batchSize(mongoConfig.getBatchSize()).fullDocument(FullDocument.UPDATE_LOOKUP);

        stream.subscribe(new Subscriber<ChangeStreamDocument<Document>>() {
            private ObjectMapper mapper = new ObjectMapper();
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                this.subscription.request(Integer.MAX_VALUE);
            }

            @Override
            public void onNext(ChangeStreamDocument<Document> doc) {
                try {
                    log.info("New change doc: {}", doc);

                    // Build a record with the essential information
                    final Map<String, Object> recordValue = new HashMap<>();
                    recordValue.put("fullDocument", doc.getFullDocument());
                    recordValue.put("ns", doc.getNamespace());
                    recordValue.put("operation", doc.getOperationType());

                    consume(new DocRecord(
                            Optional.of(doc.getDocumentKey().toJson()),
                            mapper.writeValueAsString(recordValue).getBytes(StandardCharsets.UTF_8)));

                } catch (JsonProcessingException e) {
                    log.error("Processing doc from mongo", e);
                }
            }

            @Override
            public void onError(Throwable error) {
                log.error("Subscriber error", error);
            }

            @Override
            public void onComplete() {
                log.info("Subscriber complete");
            }
        });

    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Data
    private static class DocRecord implements Record<byte[]> {
        private final Optional<String> key;
        private final byte[] value;
    }
}
