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

package org.apache.pulsar.io.aws;

import java.io.Closeable;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

/**
 * Kinesis source/sink calls credential-provider while refreshing aws accessKey and secreKey. So, implementation
 * AwsCredentialProviderPlugin needs to makes sure to return non-expired keys when it requires.
 *
 */
public interface AwsCredentialProviderPlugin extends Closeable {

    /**
     * accepts aws-account related param and initialize credential provider.
     * 
     * @param param
     */
    void init(String param);

    /**
     * Returned {@link AWSCredentialsProvider} can give {@link AWSCredentials} in case credential belongs to IAM user or
     * it can return {@link BasicSessionCredentials} if user wants to generate temporary credential for a given IAM
     * role.
     * 
     * @return
     */
    AWSCredentialsProvider getCredentialProvider();

    /**
     * Returns a V2 credential provider for use with the v2 SDK.
     *
     * Defaults to an implementation that pulls credentials from a v1 provider
     */
    default software.amazon.awssdk.auth.credentials.AwsCredentialsProvider getV2CredentialsProvider() {
        // make a small wrapper to forward requests to v1, this allows
        // for this interface to not "break" for implementers
        AWSCredentialsProvider v1Provider = getCredentialProvider();
        return () -> {
            AWSCredentials creds = v1Provider.getCredentials();
            if (creds instanceof AWSSessionCredentials) {
                return software.amazon.awssdk.auth.credentials.AwsSessionCredentials.create(
                        creds.getAWSAccessKeyId(),
                        creds.getAWSSecretKey(),
                        ((AWSSessionCredentials) creds).getSessionToken());
            } else {
                return software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                        creds.getAWSAccessKeyId(),
                        creds.getAWSSecretKey());
            }
        };
    }

}
