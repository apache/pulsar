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
package org.apache.pulsar.broker.s3offload;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import io.findify.s3mock.S3Mock;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;

public class S3TestBase {
    final static String BUCKET = "pulsar-unittest";

    S3Mock s3mock = null;
    protected AmazonS3 s3client = null;
    protected String s3endpoint = null;

    @BeforeMethod
    public void start() throws Exception {
        s3mock = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();
        int port = s3mock.start().localAddress().getPort();
        s3endpoint = "http://localhost:" + port;

        if (Boolean.parseBoolean(System.getProperty("testRealAWS", "false"))) {
            // To use this, ~/.aws must be configured with credentials and a default region
            s3client = AmazonS3ClientBuilder.standard().build();
        } else {
            s3client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(s3endpoint, "foobar"))
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withPathStyleAccessEnabled(true).build();
        }

        if (!s3client.doesBucketExistV2(BUCKET)) {
            s3client.createBucket(BUCKET);
        }
    }

    @AfterMethod
    public void stop() throws Exception {
        if (s3mock != null) {
            s3mock.shutdown();
        }
    }
}
