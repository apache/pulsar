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

import java.util.Properties;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.s3.reference.S3Constants;

/**
 * Configuration for AWS Blob storage. Used for both S3 and Glacier.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class S3BlobStoreFactory extends AWSBlobStoreFactory {

    private static final long serialVersionUID = 1L;

    // For Amazon S3 ledger off-load, Alternative endpoint to connect to (useful for testing)
    private String s3ManagedLedgerOffloadServiceEndpoint = null;
    
    private String s3ManagedLedgerOffloadRegion = null;
    
    private String s3ManagedLedgerOffloadBucket = null;
    
    @Override
    public String getRegion() {
         return s3ManagedLedgerOffloadRegion;
    }

    @Override
    public String getBucket() {
         return s3ManagedLedgerOffloadBucket;
    }

    @Override
    public String getServiceEndpoint() {
        return s3ManagedLedgerOffloadServiceEndpoint;
    }
    
    @Override
    public void setRegion(String s) {
        s3ManagedLedgerOffloadRegion = s;
    }

    @Override
    public void setServiceEndpoint(String s) {
        s3ManagedLedgerOffloadServiceEndpoint = s;
    }

    @Override
    public ContextBuilder getContextBuilder() {
        ContextBuilder builder = super.getContextBuilder();

        if (!Strings.isNullOrEmpty(s3ManagedLedgerOffloadServiceEndpoint)) {
            builder.endpoint(s3ManagedLedgerOffloadServiceEndpoint);
        }

        return builder;
    }

    @Override
    protected Properties getOverrides() {
        Properties overrides = super.getOverrides();
        if (!Strings.isNullOrEmpty(s3ManagedLedgerOffloadServiceEndpoint)) {
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }
        return overrides;
    }
}
