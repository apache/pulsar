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
package org.apache.pulsar.io.dynamodb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.testng.annotations.Test;


public class DynamoDBSourceConfigTests {

    private static final Date DAY;
    
    static {
        Calendar then = Calendar.getInstance();
        then.set(Calendar.YEAR, 2019);
        then.set(Calendar.MONTH, Calendar.MARCH);
        then.set(Calendar.DAY_OF_MONTH, 5);
        then.set(Calendar.HOUR_OF_DAY, 19);
        then.set(Calendar.MINUTE, 28);
        then.set(Calendar.SECOND, 58);
        then.set(Calendar.MILLISECOND, 0);
        then.set(Calendar.ZONE_OFFSET, 0);
        DAY = then.getTime();
    }

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sourceConfig.yaml");
        DynamoDBSourceConfig config = DynamoDBSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals(config.getAwsEndpoint(), "https://some.endpoint.aws");
        assertEquals(config.getAwsRegion(), "us-east-1");
        assertEquals(config.getAwsDynamodbStreamArn(), "arn:aws:dynamodb:us-west-2:111122223333:table/TestTable/stream/2015-05-11T21:21:33.291");
        assertEquals(config.getAwsCredentialPluginParam(),
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        assertEquals(config.getApplicationName(), "My test application");
        assertEquals(config.getCheckpointInterval(), 30000);
        assertEquals(config.getBackoffTime(), 4000);
        assertEquals(config.getNumRetries(), 3);
        assertEquals(config.getReceiveQueueSize(), 2000);
        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(config.getStartAtTime());
        ZonedDateTime actual = ZonedDateTime.ofInstant(cal.toInstant(), ZoneOffset.UTC);
        ZonedDateTime expected = ZonedDateTime.ofInstant(DAY.toInstant(), ZoneOffset.UTC);
        assertEquals(actual, expected);
    }
    
    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("awsEndpoint", "https://some.endpoint.aws");
        map.put("awsRegion", "us-east-1");
        map.put("awsDynamodbStreamArn", "arn:aws:dynamodb:us-west-2:111122223333:table/TestTable/stream/2015-05-11T21:21:33.291");
        map.put("awsCredentialPluginParam", "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        map.put("checkpointInterval", "30000");
        map.put("backoffTime", "4000");
        map.put("numRetries", "3");
        map.put("receiveQueueSize", 2000);
        map.put("applicationName", "My test application");
        map.put("initialPositionInStream", InitialPositionInStream.TRIM_HORIZON);
        map.put("startAtTime", DAY);

        DynamoDBSourceConfig config = DynamoDBSourceConfig.load(map);
        
        assertNotNull(config);
        assertEquals(config.getAwsEndpoint(), "https://some.endpoint.aws");
        assertEquals(config.getAwsRegion(), "us-east-1");
        assertEquals(config.getAwsDynamodbStreamArn(), "arn:aws:dynamodb:us-west-2:111122223333:table/TestTable/stream/2015-05-11T21:21:33.291");
        assertEquals(config.getAwsCredentialPluginParam(),
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        assertEquals(config.getApplicationName(), "My test application");
        assertEquals(config.getCheckpointInterval(), 30000);
        assertEquals(config.getBackoffTime(), 4000);
        assertEquals(config.getNumRetries(), 3);
        assertEquals(config.getReceiveQueueSize(), 2000);
        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(config.getStartAtTime());
        ZonedDateTime actual = ZonedDateTime.ofInstant(cal.toInstant(), ZoneOffset.UTC);
        ZonedDateTime expected = ZonedDateTime.ofInstant(DAY.toInstant(), ZoneOffset.UTC);
        assertEquals(actual, expected);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "empty aws-credential param")
    public final void missingCredentialsTest() throws Exception {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("awsEndpoint", "https://some.endpoint.aws");
        map.put("awsRegion", "us-east-1");
        map.put("awsDynamodbStreamArn", "arn:aws:dynamodb:us-west-2:111122223333:table/TestTable/stream/2015-05-11T21:21:33.291");
     
        DynamoDBSource source = new DynamoDBSource();
        source.open(map, null);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Timestamp must be specified")
    public final void missingStartTimeTest() throws Exception {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("awsEndpoint", "https://some.endpoint.aws");
        map.put("awsRegion", "us-east-1");
        map.put("awsDynamodbStreamArn", "arn:aws:dynamodb:us-west-2:111122223333:table/TestTable/stream/2015-05-11T21:21:33.291");
        map.put("awsCredentialPluginParam", 
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        map.put("initialPositionInStream", InitialPositionInStream.AT_TIMESTAMP);

        DynamoDBSource source = new DynamoDBSource();
        source.open(map, null);
    }
    
    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
