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

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Lists;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.bson.BSONException;
import org.bson.Document;
import org.bson.json.JsonParseException;

/**
 * The base class for MongoDB sinks.
 * Users need to implement extractKeyValue function to use this sink.
 * This class assumes that the input will be JSON documents.
 */
@Connector(
    name = "mongo",
    type = IOType.SINK,
    help = "A sink connector that sends pulsar messages to mongodb",
    configClass = MongoConfig.class
)
@Slf4j
public class MongoSink implements Sink<byte[]> {

    private MongoConfig mongoConfig;

    private MongoClient mongoClient;

    private MongoCollection<Document> collection;

    private List<Record<byte[]>> incomingList;

    private ScheduledExecutorService flushExecutor;

    private Supplier<MongoClient> clientProvider;

    public MongoSink() {
        this(null);
    }

    public MongoSink(Supplier<MongoClient> clientProvider) {
        this.clientProvider = clientProvider;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Open MongoDB Sink");

        mongoConfig = MongoConfig.load(config);
        mongoConfig.validate();

        if (clientProvider != null) {
            mongoClient = clientProvider.get();
        } else {
            mongoClient = MongoClients.create(mongoConfig.getMongoUri());
        }

        final MongoDatabase db = mongoClient.getDatabase(mongoConfig.getDatabase());
        collection = db.getCollection(mongoConfig.getCollection());

        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(),
                mongoConfig.getBatchTimeMs(), mongoConfig.getBatchTimeMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(Record<byte[]> record) {
        final String recordValue = new String(record.getValue(), Charset.forName("UTF-8"));

        if (log.isDebugEnabled()) {
            log.debug("Received record: " + recordValue);
        }

        int currentSize;

        synchronized (this) {
            incomingList.add(record);
            currentSize = incomingList.size();
        }

        if (currentSize == mongoConfig.getBatchSize()) {
            flushExecutor.submit(() -> flush());
        }
    }

    private void flush() {
        final List<Document> docsToInsert = new ArrayList<>();
        final List<Record<byte[]>> recordsToInsert;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }

            recordsToInsert = incomingList;
            incomingList = Lists.newArrayList();
        }

        final Iterator<Record<byte[]>> iter = recordsToInsert.iterator();

        while (iter.hasNext()) {
            final Record<byte[]> record = iter.next();

            try {
                final byte[] docAsBytes = record.getValue();
                final Document doc = Document.parse(new String(docAsBytes, Charset.forName("UTF-8")));
                docsToInsert.add(doc);
            }
            catch (JsonParseException | BSONException e) {
                log.error("Bad message", e);
                record.fail();
                iter.remove();
            }
        }

        if (docsToInsert.size() > 0) {

            collection.insertMany(docsToInsert, (result, t) -> {
                final List<Integer> idxToAck = IntStream.range(0, docsToInsert.size()).boxed().collect(toList());
                final List<Integer> idxToFail = Lists.newArrayList();

                if (t != null) {
                    log.error("MongoDB insertion error", t);

                    if (t instanceof MongoBulkWriteException) {
                        // With this exception, we are aware of the items that have not been inserted.
                        ((MongoBulkWriteException) t).getWriteErrors().forEach(err -> {
                            idxToFail.add(err.getIndex());
                        });
                        idxToAck.removeAll(idxToFail);
                    } else {
                        idxToFail.addAll(idxToAck);
                        idxToAck.clear();
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("Nb ack={}, nb fail={}", idxToAck.size(), idxToFail.size());
                }

                idxToAck.forEach(idx -> recordsToInsert.get(idx).ack());
                idxToFail.forEach(idx -> recordsToInsert.get(idx).fail());
            });
        }
    }

    @Override
    public void close() throws Exception {
        if (flushExecutor != null) {
            flushExecutor.shutdown();
        }

        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
