package org.apache.pulsar.io.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsGetSessionTokenCredentialsProvider;

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
