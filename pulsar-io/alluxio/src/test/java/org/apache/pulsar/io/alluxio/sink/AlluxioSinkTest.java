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
package org.apache.pulsar.io.alluxio.sink;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioCluster;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Alluxio Sink test
 */
@Slf4j
public class AlluxioSinkTest {

    @Mock
    protected SinkContext mockSinkContext;

    protected Map<String, Object> map;
    protected AlluxioSink sink;
    protected LocalAlluxioCluster cluster;

    @Mock
    protected Record<GenericObject> mockRecord;

    static Schema kvSchema;
    static Schema<Foobar> valueSchema;
    static GenericSchema<GenericRecord> genericSchema;
    static GenericRecord fooBar;

    @BeforeClass
    public static void init() {
        valueSchema = Schema.JSON(Foobar.class);
        genericSchema = Schema.generic(valueSchema.getSchemaInfo());
        fooBar = genericSchema.newRecordBuilder()
                .set("name", "foo")
                .set("address", "foobar")
                .set("age", 20)
                .build();
        kvSchema = Schema.KeyValue(Schema.STRING, genericSchema, KeyValueEncodingType.SEPARATED);
    }

    @BeforeMethod
    public final void setUp() throws Exception {
        cluster = setupSingleMasterCluster();

        map = new HashMap<>();
        // alluxioMasterHost should be set via LocalAlluxioCluster#getHostname
        // instead of using a fixed value "localhost", since it seems that
        // LocalAlluxioCluster may bind other address than localhost
        // when the node has multiple network interfaces.
        map.put("alluxioMasterHost", cluster.getHostname());
        map.put("alluxioMasterPort", cluster.getMasterRpcPort());
        map.put("alluxioDir", "/pulsar");
        map.put("filePrefix", "prefix");
        map.put("schemaEnable", "true");

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            int count = 0;
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                return Optional.of( "key-" + count++);
            }});

        when(mockRecord.getValue()).thenAnswer((Answer<GenericObject>) invocation -> new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return new KeyValue<String, GenericObject>((String) fooBar.getField("address"), fooBar);
            }
        });

        when(mockRecord.getSchema()).thenAnswer((Answer<Schema<KeyValue<String, Foobar>>>) invocation -> kvSchema);
    }

    @Test
    public void openTest() throws Exception {
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");

        String alluxioDir = "/pulsar";

        sink = new AlluxioSink();
        sink.open(map, mockSinkContext);

        FileSystem client = cluster.getClient();

        AlluxioURI alluxioURI = new AlluxioURI(alluxioDir);
        Assert.assertTrue(client.exists(alluxioURI));

        String alluxioTmpDir = FilenameUtils.concat(alluxioDir, "tmp");
        AlluxioURI alluxioTmpURI = new AlluxioURI(alluxioTmpDir);
        Assert.assertTrue(client.exists(alluxioTmpURI));

        sink.close();
        cluster.stop();
    }

    @Test
    public void writeAndCloseTest() throws Exception {
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "1");
        map.put("writeType", "THROUGH");
        map.put("alluxioDir", "/pulsar");

        String alluxioDir = "/pulsar";

        sink = new AlluxioSink();
        sink.open(map, mockSinkContext);

        sink.write(() -> new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return new KeyValue<>((String) fooBar.getField("address"), fooBar);
            }
        });

        FileSystem client = cluster.getClient();

        AlluxioURI alluxioURI = new AlluxioURI(alluxioDir);
        Assert.assertTrue(client.exists(alluxioURI));

        String alluxioTmpDir = FilenameUtils.concat(alluxioDir, "tmp");
        AlluxioURI alluxioTmpURI = new AlluxioURI(alluxioTmpDir);
        Assert.assertTrue(client.exists(alluxioTmpURI));

        List<URIStatus> listAlluxioDirStatus = client.listStatus(alluxioURI);

        List<String> pathList = listAlluxioDirStatus.stream().map(URIStatus::getPath).collect(Collectors.toList());

        Assert.assertEquals(pathList.size(), 2);

        for (String path : pathList) {
            if (path.contains("tmp")) {
                Assert.assertEquals(path, "/pulsar/tmp");
            } else {
                Assert.assertTrue(path.startsWith("/pulsar/TopicA-"));
            }
        }

        sink.close();
        cluster.stop();
    }

    private LocalAlluxioCluster setupSingleMasterCluster() throws Exception {
        // Setup and start the local alluxio cluster
        LocalAlluxioCluster cluster = new LocalAlluxioCluster();
        cluster.initConfiguration(getTestName(getClass().getSimpleName(), LocalAlluxioCluster.DEFAULT_TEST_NAME));
        ServerConfiguration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE);
        cluster.start();
        return cluster;
    }

    public String getTestName(String className, String methodName) {
        String testName = className + "-" + methodName;
        // cannot use these characters in the name/path: . [ ]
        testName = testName.replace(".", "-");
        testName = testName.replace("[", "-");
        testName = testName.replace("]", "");
        return testName;
    }
}
