package org.apache.pulsar.io.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsGetSessionTokenCredentialsProvider;

import java.io.IOException;

public class AwsDefaultProviderChainPlugin implements AwsCredentialProviderPlugin {
    @Override
    public void init(String param) {

    }

    @Override
    public AWSCredentialsProvider getCredentialProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }

    @Override
    public software.amazon.awssdk.auth.credentials.AwsCredentialsProvider getV2CredentialsProvider() {
        return DefaultCredentialsProvider.create();
    }

    @Override
    public void close() throws IOException {

    }
}
