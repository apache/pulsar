package org.apache.bookkeeper.mledger.offload.jclouds.provider.factory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Strings;

import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.providers.ProviderMetadata;

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
