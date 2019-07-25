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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.bulk.BulkWriteError;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.bson.BsonDocument;
import org.mockito.Mock;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

public class MongoSinkTest {

    @Mock
    private Record<byte[]> mockRecord;

    @Mock
    private SinkContext mockSinkContext;

    @Mock
    private MongoClient mockMongoClient;

    @Mock
    private MongoDatabase mockMongoDb;

    @Mock
    private MongoCollection mockMongoColl;

    private MongoSink sink;

    private Map<String, Object> map;


    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @BeforeMethod
    public void setUp() {

        map = TestHelper.createMap(true);

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);
        mockMongoClient = mock(MongoClient.class);
        mockMongoDb = mock(MongoDatabase.class);
        mockMongoColl = mock(MongoCollection.class);

        sink = new MongoSink(() -> mockMongoClient);

        when(mockMongoClient.getDatabase(anyString())).thenReturn(mockMongoDb);
        when(mockMongoDb.getCollection(anyString())).thenReturn(mockMongoColl);
    }

    private void initContext(boolean throwBulkError) {
        when(mockRecord.getValue()).thenReturn("{\"hello\":\"pulsar\"}".getBytes());

        doAnswer((invocation) -> {
            SingleResultCallback cb = invocation.getArgument(1, SingleResultCallback.class);
            MongoBulkWriteException exc = null;

            if (throwBulkError) {
                List<BulkWriteError > writeErrors = Arrays.asList(
                        new BulkWriteError(0, "error", new BsonDocument(), 1));
                exc = new MongoBulkWriteException(null, writeErrors, null, null);
            }

            cb.onResult(null, exc);
            return null;
        }).when(mockMongoColl).insertMany(any(), any());
    }

    private void initFailContext(String msg) {
        when(mockRecord.getValue()).thenReturn(msg.getBytes());

        doAnswer((invocation) -> {
            SingleResultCallback cb = invocation.getArgument(1, SingleResultCallback.class);
            cb.onResult(null, new Exception("Oops"));
            return null;
        }).when(mockMongoColl).insertMany(any(), any());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        sink.close();
        verify(mockMongoClient, times(1)).close();
    }

    @Test
    public void testOpen() throws Exception {
        sink.open(map, mockSinkContext);
    }

    @Test
    public void testWriteNullMessage() throws Exception {
        when(mockRecord.getValue()).thenReturn("".getBytes());

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteGoodMessage() throws Exception {
        initContext(false);

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).ack();
    }

    @Test
    public void testWriteMultipleMessages() throws Exception {
        initContext(true);

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);
        sink.write(mockRecord);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(2)).ack();
        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteWithError() throws Exception {
        initFailContext("{\"hello\":\"pulsar\"}");

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteBadMessage() throws Exception {
        initFailContext("Oops");

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }
}
