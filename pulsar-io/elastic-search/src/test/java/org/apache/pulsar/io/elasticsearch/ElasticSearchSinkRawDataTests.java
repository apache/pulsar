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
package org.apache.pulsar.io.elasticsearch;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.AfterClass;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ElasticSearchSinkRawDataTests {

    public static final String ELASTICSEARCH_IMAGE = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2-amd64");

    private static ElasticsearchContainer container;

    @Mock
    protected Record<GenericObject> mockRecord;

    @Mock
    protected Message mockMessage;

    @Mock
    protected SinkContext mockSinkContext;
    protected Map<String, Object> map;
    protected ElasticSearchSink sink;

    static Schema<byte[]> schema;

    @BeforeClass
    public static final void initBeforeClass() {
        container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
        schema = Schema.BYTES;
    }

    @AfterClass
    public static void closeAfterClass() {
        container.close();
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {
        container.start();

        map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://"+container.getHttpHostAddress());
        map.put("schemaEnable", "false");
        sink = new ElasticSearchSink();

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);
        mockMessage = mock(Message.class);

        when(mockMessage.getData()).thenReturn("{\"a\":\"b\"}".getBytes(StandardCharsets.UTF_8));

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                throw new RuntimeException("Not expected to be called");
            }});


        when(mockRecord.getValue()).thenAnswer(new Answer<GenericObject>() {
            public GenericObject answer(InvocationOnMock invocation) throws Throwable {
                throw new RuntimeException("Not expected to be called");
            }});

        when(mockRecord.getSchema()).thenAnswer(new Answer<Schema>() {
            public Schema answer(InvocationOnMock invocation) throws Throwable {
                return schema;
            }});

        when(mockRecord.getMessage()).thenReturn(Optional.of(mockMessage));
    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown() throws Exception {
        if (sink != null)
            sink.close();
    }

    @Test(enabled = true)
    public final void singleNonSchemaAwareTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(10);
        verify(mockRecord, times(10)).ack();
        verify(mockMessage, times(10)).getData();
        verify(mockMessage, times(0)).getValue();
        verify(mockRecord, times(0)).getValue();
        verify(mockRecord, times(0)).getKey();
    }

    protected final void send(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            sink.write(mockRecord);
        }
    }

}
