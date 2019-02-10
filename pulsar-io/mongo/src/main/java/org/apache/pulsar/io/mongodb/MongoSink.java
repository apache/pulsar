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

import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.bson.BSONException;
import org.bson.Document;
import org.bson.json.JsonParseException;

import java.util.Map;

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


    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Open MongoDB Sink");
        mongoConfig = MongoConfig.load(config);
        mongoConfig.validate();

        mongoClient = MongoClients.create(mongoConfig.getMongoUri());
    }

    @Override
    public void write(Record<byte[]> record) {
        log.info("Received record: " + new String(record.getValue()));
        try {
            final byte[] docAsBytes = record.getValue();
            final Document doc = Document.parse(new String(docAsBytes));
            final MongoDatabase db = mongoClient.getDatabase(mongoConfig.getDatabase());
            final MongoCollection<Document> collection = db.getCollection(mongoConfig.getCollection());

            collection.insertOne(doc, (Void result, final Throwable t) -> {
                if (t == null) {
                    record.ack();
                } else {
                    log.error("MongoDB insertion error", t);
                    record.fail();
                }
            });
        }
        catch (JsonParseException | BSONException e) {
            log.error("Bad message", e);
            record.fail();
        }
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
