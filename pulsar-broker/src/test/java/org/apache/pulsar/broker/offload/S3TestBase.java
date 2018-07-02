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
package org.apache.pulsar.broker.offload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.testng.annotations.BeforeMethod;

public class S3TestBase {
    public final static String BUCKET = "pulsar-unittest";

    protected AmazonS3 s3client = null;

    @BeforeMethod
    public void start() throws Exception {
        if (Boolean.parseBoolean(System.getProperty("testRealAWS", "false"))) {
            // To use this, ~/.aws must be configured with credentials and a default region
            s3client = AmazonS3ClientBuilder.standard().build();
        } else {
            s3client = new S3Mock();
        }

        if (!s3client.doesBucketExistV2(BUCKET)) {
            s3client.createBucket(BUCKET);
        }
    }

}
