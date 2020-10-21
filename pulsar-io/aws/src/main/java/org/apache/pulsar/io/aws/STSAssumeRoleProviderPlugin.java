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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.IOException;
import java.util.Map;

public class STSAssumeRoleProviderPlugin implements AwsCredentialProviderPlugin {
    public static final String ASSUME_ROLE_ARN = "roleArn";
    public static final String ASSUME_ROLE_SESSION_NAME = "roleSessionName";

    private String roleArn;
    private String roleSessionName;

    @Override
    public void init(String param) {
        Map<String, String> credentialMap = new Gson().fromJson(param,
                new TypeToken<Map<String, String>>() {
                }.getType());

        roleArn = credentialMap.get(ASSUME_ROLE_ARN);
        roleSessionName = credentialMap.get(ASSUME_ROLE_SESSION_NAME);
    }

    @Override
    public AWSCredentialsProvider getCredentialProvider() {
        return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName).build();
    }

    @Override
    public software.amazon.awssdk.auth.credentials.AwsCredentialsProvider getV2CredentialsProvider() {
        StsClient client = StsClient.create();
        return StsAssumeRoleCredentialsProvider.builder().stsClient(client).refreshRequest((req) -> {
            req.roleArn(roleArn).roleSessionName(roleSessionName).build();
        }).build();
    }

    @Override
    public void close() throws IOException {
    }
}
