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
package org.apache.pulsar.io.kinesis;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.kinesis.KinesisSinkConfig.MessageFormat;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class KinesisSinkConfigTests {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        KinesisSinkConfig config = KinesisSinkConfig.load(yamlFile.getAbsolutePath());

        assertNotNull(config);
        assertEquals(config.getAwsEndpoint(), "https://some.endpoint.aws");
        assertEquals(config.getAwsRegion(), "us-east-1");
        assertEquals(config.getAwsKinesisStreamName(), "my-stream");
        assertEquals(config.getAwsCredentialPluginParam(),
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        assertEquals(config.getMessageFormat(), MessageFormat.ONLY_RAW_PAYLOAD);
        assertEquals(true, config.isRetainOrdering());
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("awsEndpoint", "https://some.endpoint.aws");
        map.put("awsRegion", "us-east-1");
        map.put("awsKinesisStreamName", "my-stream");
        map.put("awsCredentialPluginParam", "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        KinesisSinkConfig config = IOConfigUtils.loadWithSecrets(map, KinesisSinkConfig.class, sinkContext);

        assertNotNull(config);
        assertEquals(config.getAwsEndpoint(), "https://some.endpoint.aws");
        assertEquals(config.getAwsRegion(), "us-east-1");
        assertEquals(config.getAwsKinesisStreamName(), "my-stream");
        assertEquals(config.getAwsCredentialPluginParam(),
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
    }

    @Test
    public final void loadFromMapCredentialFromSecretTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("awsEndpoint", "https://some.endpoint.aws");
        map.put("awsRegion", "us-east-1");
        map.put("awsKinesisStreamName", "my-stream");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        Mockito.when(sinkContext.getSecret("awsCredentialPluginParam"))
                .thenReturn("{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        KinesisSinkConfig config = IOConfigUtils.loadWithSecrets(map, KinesisSinkConfig.class, sinkContext);

        assertNotNull(config);
        assertEquals(config.getAwsEndpoint(), "https://some.endpoint.aws");
        assertEquals(config.getAwsRegion(), "us-east-1");
        assertEquals(config.getAwsKinesisStreamName(), "my-stream");
        assertEquals(config.getAwsCredentialPluginParam(),
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
