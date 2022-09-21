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

package org.apache.pulsar.tests.integration.io.sources;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.DebeziumMongoDbContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MongoDBContainer;

@Slf4j
public class MongoSourceTester extends SourceTester<MongoDBContainer> {

    private static final String SOURCE_TYPE = "mongo";

    private static final String DEFAULT_DATABASE = "test";

    private static final int DEFAULT_BATCH_SIZE = 2;

    private final MongoDBContainer mongoContainer;

    private final PulsarCluster pulsarCluster;

    protected MongoSourceTester(MongoDBContainer mongoContainer, PulsarCluster pulsarCluster) {
        super(SOURCE_TYPE);
        this.mongoContainer = mongoContainer;
        this.pulsarCluster = pulsarCluster;

        sourceConfig.put("mongoUri", mongoContainer.getConnectionString());
        sourceConfig.put("database", DEFAULT_DATABASE);
        sourceConfig.put("syncType", "full_sync");
        sourceConfig.put("batchSize", DEFAULT_BATCH_SIZE);
    }

    @Override
    public void setServiceContainer(MongoDBContainer serviceContainer) {
        log.info("start mongodb server container.");
        pulsarCluster.startService(DebeziumMongoDbContainer.NAME, mongoContainer);
    }

    @Override
    public void prepareSource() throws Exception {
        MongoClient mongoClient = MongoClients.create(mongoContainer.getConnectionString());
        MongoDatabase db = mongoClient.getDatabase(DEFAULT_DATABASE);
        log.info("Subscribing mongodb change streams on: {}", mongoContainer.getReplicaSetUrl(DEFAULT_DATABASE));

        ChangeStreamPublisher<Document> stream = db.watch();
        stream.batchSize(DEFAULT_BATCH_SIZE)
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        stream.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(Integer.MAX_VALUE);
            }

            @Override
            public void onNext(ChangeStreamDocument<Document> doc) {
                log.info("New change doc: {}", doc);
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

        log.info("Successfully subscribe to mongodb change streams");
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        Container.ExecResult execResult = this.mongoContainer.execInContainer(
                "/usr/bin/mongo",
                "--eval",
                "db.products.insert" +
                        "({" +
                        "name: \"test-mongo\"," +
                        "description: \"test message\"" +
                        "})"
        );
        log.info("Successfully insert a message: {}", execResult.getStdout());
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        Container.ExecResult execResult = mongoContainer.execInContainer(
                "/usr/bin/mongo",
                "--eval",
                "db.products.deleteOne" +
                        "({" +
                        "name: \"test-mongo\"" +
                        "})"
        );
        log.info("Successfully delete a message: {}", execResult.getStdout());
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        Container.ExecResult execResult = mongoContainer.execInContainer(
                "/usr/bin/mongo",
                "--eval",
                "db.products.update" +
                        "(" +
                        "{name: \"test-mongo-source\"}" +
                        "," +
                        "{$set:{name:\"test-mongo-update\", description: \"updated message\"}}" +
                        ")"
        );
        log.info("Successfully update a message: {}", execResult.getStdout());
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        log.info("mongodb server already contains preconfigured data.");
        return null;
    }

    @Override
    public void close() throws Exception {
        if (mongoContainer != null) {
            mongoContainer.close();
        }
    }
}
