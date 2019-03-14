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
package org.apache.pulsar.io.alluxio.sink;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.master.LocalAlluxioCluster;
import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Alluxio String Sink test
 */
@Slf4j
public class AlluxioStringSinkTest {

    private static final String ALLUXIO_WEB_APTH = "core/server/common/src/main/webapp";

    @Mock
    protected SinkContext mockSinkContext;

    protected Map<String, Object> map;
    protected AlluxioAbstractSink<String, String> sink;

    @BeforeMethod
    public final void setUp() throws Exception {
        map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("alluxioDir", "/pulsar");
        map.put("filePrefix", "prefix");

        mockSinkContext = mock(SinkContext.class);

        createAlluxioWebPath();
    }

    @Test
    public void openTest() throws Exception {
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");

        String alluxioDir = "/pulsar";

        LocalAlluxioCluster cluster = setupSingleMasterCluster();

        sink = new AlluxioStringSink();
        sink.open(map, mockSinkContext);

        FileSystem client = cluster.getClient();

        AlluxioURI alluxioURI = new AlluxioURI(alluxioDir);
        Assert.assertTrue(client.exists(alluxioURI));

        String alluxioTmpDir = FilenameUtils.concat(alluxioDir, "tmp");
        AlluxioURI alluxioTmpURI = new AlluxioURI(alluxioTmpDir);
        Assert.assertTrue(client.exists(alluxioTmpURI));
    }

    @Test
    public void writeAndCloseTest() throws Exception {
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");
        map.put("writeType", "THROUGH");

        String alluxioDir = "/pulsar";

        LocalAlluxioCluster cluster = setupSingleMasterCluster();

        sink = new AlluxioStringSink();
        sink.open(map, mockSinkContext);

        List<Record<String>> records = SinkRecordHelper.buildBatch(10);
        records.forEach(record -> sink.write(record));

        FileSystem client = cluster.getClient();

        AlluxioURI alluxioURI = new AlluxioURI(alluxioDir);
        Assert.assertTrue(client.exists(alluxioURI));
        List<URIStatus> listAlluxioDirStatus = client.listStatus(alluxioURI);

        Assert.assertEquals(listAlluxioDirStatus.size(), 1);

        String alluxioTmpDir = FilenameUtils.concat(alluxioDir, "tmp");
        AlluxioURI alluxioTmpURI = new AlluxioURI(alluxioTmpDir);
        Assert.assertTrue(client.exists(alluxioTmpURI));
        List<URIStatus> listAlluxioTmpDirStatus = client.listStatus(alluxioTmpURI);

        Assert.assertEquals(listAlluxioTmpDirStatus.size(), 1);

        sink.close();
        cluster.stop();
    }

    @Test
    public void rotateTest() throws Exception {
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "10");
        map.put("writeType", "THROUGH");

        String alluxioDir = "/pulsar";

        LocalAlluxioCluster cluster = setupSingleMasterCluster();

        sink = new AlluxioStringSink();
        sink.open(map, mockSinkContext);

        List<Record<String>> records = SinkRecordHelper.buildBatch(13);
        records.forEach(record -> sink.write(record));

        FileSystem client = cluster.getClient();

        AlluxioURI alluxioURI = new AlluxioURI(alluxioDir);
        Assert.assertTrue(client.exists(alluxioURI));
        List<URIStatus> listAlluxioDirStatus = client.listStatus(alluxioURI);

        Assert.assertEquals(listAlluxioDirStatus.size(), 2);

        String alluxioTmpDir = FilenameUtils.concat(alluxioDir, "tmp");
        AlluxioURI alluxioTmpURI = new AlluxioURI(alluxioTmpDir);
        Assert.assertTrue(client.exists(alluxioTmpURI));
        List<URIStatus> listAlluxioTmpDirStatus = client.listStatus(alluxioTmpURI);

        Assert.assertEquals(listAlluxioTmpDirStatus.size(), 1);
    }

    private LocalAlluxioCluster setupSingleMasterCluster() throws Exception {
        // Setup and start the local alluxio cluster
        LocalAlluxioCluster cluster = new LocalAlluxioCluster();
        cluster.initConfiguration();
        Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE);
        cluster.start();
        return cluster;
    }

    private void createAlluxioWebPath() throws Exception {
        // Create alluxio web path if not exist
        String classPath = Resources.getResource("").getPath();
        String parentPath = new File(classPath).getParentFile().getAbsolutePath();
        String alluxioWebPath = FilenameUtils.concat(parentPath, ALLUXIO_WEB_APTH);
        File alluxioWebFile = new File(alluxioWebPath);
        if (!alluxioWebFile.exists()) {
            alluxioWebFile.mkdirs();
        }
    }
}
