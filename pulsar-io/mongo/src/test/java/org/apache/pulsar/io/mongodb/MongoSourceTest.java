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

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.mockito.Mock;
import org.reactivestreams.Subscriber;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;


import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.testng.Assert.assertEquals;

public class MongoSourceTest {

    @Mock
    private SourceContext mockSourceContext;

    @Mock
    private MongoClient mockMongoClient;

    @Mock
    private MongoDatabase mockMongoDb;

    @Mock
    private MongoCollection mockMongoColl;

    @Mock
    private ChangeStreamPublisher mockPublisher;

    private Subscriber subscriber;

    private MongoSource source;

    private Map<String, Object> map;

    @BeforeMethod
    public void setUp() {

        map = TestHelper.createMap(true);

        mockSourceContext = mock(SourceContext.class);
        mockMongoClient = mock(MongoClient.class);
        mockMongoDb = mock(MongoDatabase.class);
        mockMongoColl = mock(MongoCollection.class);
        mockPublisher = mock(ChangeStreamPublisher.class);

        source = new MongoSource(() -> mockMongoClient);

        when(mockMongoClient.getDatabase(anyString())).thenReturn(mockMongoDb);
        when(mockMongoDb.getCollection(anyString())).thenReturn(mockMongoColl);
        when(mockMongoColl.watch()).thenReturn(mockPublisher);
        when(mockPublisher.batchSize(anyInt())).thenReturn(mockPublisher);
        when(mockPublisher.fullDocument(any())).thenReturn(mockPublisher);

        doAnswer((invocation) -> {
            subscriber = invocation.getArgument(0, Subscriber.class);
            return null;
        }).when(mockPublisher).subscribe(any());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        source.close();
        verify(mockMongoClient, times(1)).close();
    }

    @Test
    public void testOpen() throws Exception {
        source.open(map, mockSourceContext);
    }

    @Test
    public void testWriteBadMessage() throws Exception {

        source.open(map, mockSourceContext);

        subscriber.onNext(new ChangeStreamDocument<>(
                OperationType.INSERT,
                BsonDocument.parse("{token: true}"),
                BsonDocument.parse("{db: \"hello\", coll: \"pulsar\"}"),
                BsonDocument.parse("{db: \"hello2\", coll: \"pulsar2\"}"),
                new Document("hello", "pulsar"),
                BsonDocument.parse("{_id: 1}"),
                new BsonTimestamp(1234, 2),
                null,
                new BsonInt64(1),
                BsonDocument.parse("{id: 1, uid: 1}")));

        Record<byte[]> record = source.read();

        assertEquals(new String(record.getValue()),
                "{\"fullDocument\":{\"hello\":\"pulsar\"},"
                + "\"ns\":{\"databaseName\":\"hello\",\"collectionName\":\"pulsar\",\"fullName\":\"hello.pulsar\"},"
                + "\"operation\":\"INSERT\"}");
    }
}
