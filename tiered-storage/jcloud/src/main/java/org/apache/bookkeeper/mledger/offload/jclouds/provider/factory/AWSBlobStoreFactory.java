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
package org.apache.bookkeeper.mledger.offload.jclouds.provider.factory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Strings;

import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.providers.ProviderMetadata;

/**
 * Configuration for AWS Blob storage.
 */
public abstract class AWSBlobStoreFactory extends JCloudBlobStoreFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public void validate() {
        if (Strings.isNullOrEmpty(getRegion()) && Strings.isNullOrEmpty(getServiceEndpoint())) {
            throw new IllegalArgumentException(
                    "Either Region or ServiceEndpoint must be set if AWS offload enabled");
        }

        if (Strings.isNullOrEmpty(getBucket())) {
            throw new IllegalArgumentException(
                "Bucket cannot be empty for AWS offload");
        }

        if (maxBlockSizeInBytes < 5 * MB) {
            throw new IllegalArgumentException(
                "ManagedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB for s3 offload");
        }
    }

    @Override
    public Credentials getCredentials() {
        if (credentials == null) {
            AWSCredentials awsCredentials = null;
            try {
                DefaultAWSCredentialsProviderChain creds = DefaultAWSCredentialsProviderChain.getInstance();
                awsCredentials = creds.getCredentials();
            } catch (Exception e) {
                // allowed, some mock s3 service do not need credential
                LOG.warn("Exception when get credentials for s3 ", e);
            }

            String id = "accesskey";
            String key = "secretkey";
            if (awsCredentials != null) {
                id = awsCredentials.getAWSAccessKeyId();
                key = awsCredentials.getAWSSecretKey();
            }
            credentials = new Credentials(id, key);
        }
        return credentials;
    }

    @Override
    public ProviderMetadata getProviderMetadata() {
        return new AWSS3ProviderMetadata();
    }
}
