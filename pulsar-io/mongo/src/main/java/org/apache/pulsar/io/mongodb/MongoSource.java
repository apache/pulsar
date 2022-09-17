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
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * The base class for MongoDB sources.
 */
@Connector(
        name = "mongo",
        type = IOType.SOURCE,
        help = "A source connector that sends mongodb documents to pulsar",
        configClass = MongoSourceConfig.class
)
@Slf4j
public class MongoSource extends PushSource<byte[]> {

    private final Supplier<MongoClient> clientProvider;

    private MongoSourceConfig mongoSourceConfig;

    private MongoClient mongoClient;

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

        mongoSourceConfig = MongoSourceConfig.load(config);
        mongoSourceConfig.validate();

        if (clientProvider != null) {
            mongoClient = clientProvider.get();
        } else {
            mongoClient = MongoClients.create(mongoSourceConfig.getMongoUri());
        }

        String mongoDatabase = mongoSourceConfig.getDatabase();
        if (StringUtils.isEmpty(mongoDatabase)) {
            // Watch all databases
            log.info("Watch all databases");
            stream = mongoClient.watch();

        } else {
            final MongoDatabase db = mongoClient.getDatabase(mongoDatabase);
            String mongoCollection = mongoSourceConfig.getCollection();
            if (StringUtils.isEmpty(mongoCollection)) {
                // Watch all collections in a database
                log.info("Watch db: {}", db.getName());
                stream = db.watch();

            } else {
                // Watch a collection
                final MongoCollection<Document> collection = db.getCollection(mongoCollection);
                log.info("Watch collection: {}.{}", db.getName(), mongoCollection);
                stream = collection.watch();
            }
        }

        stream.batchSize(mongoSourceConfig.getBatchSize())
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        if (mongoSourceConfig.getSyncType() == SyncType.FULL_SYNC) {
            // sync currently existing messages
            // startAtOperationTime is the starting point for the change stream
            // setting startAtOperationTime to 0 means the start point is the earliest
            // see https://www.mongodb.com/docs/v4.2/reference/method/db.collection.watch/ for more information
            stream.startAtOperationTime(new BsonTimestamp(0L));
        }

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

                    BsonDocument documentKey = doc.getDocumentKey();
                    if (documentKey == null) {
                        log.warn("The document key is null");
                        return;
                    }

                    // Build a record with the essential information
                    final Map<String, Object> recordValue = new HashMap<>();
                    recordValue.put("fullDocument", doc.getFullDocument());
                    recordValue.put("ns", doc.getNamespace());
                    recordValue.put("operation", doc.getOperationType());

                    consume(new DocRecord(
                            Optional.of(documentKey.toJson()),
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
