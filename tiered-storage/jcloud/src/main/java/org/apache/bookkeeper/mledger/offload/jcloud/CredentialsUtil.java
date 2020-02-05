package org.apache.bookkeeper.mledger.offload.jcloud;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.common.base.Strings;
import org.apache.pulsar.common.policies.data.OffloadPolicies;

public class CredentialsUtil {

    /**
     * Builds an AWS credential provider based on the offload options
     * @return aws credential provider
     */
    public static AWSCredentialsProvider getAWSCredentialProvider(OffloadPolicies offloadPolicies) {
        if (Strings.isNullOrEmpty(offloadPolicies.getS3ManagedLedgerOffloadRole())) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        } else {
            String roleName = offloadPolicies.getS3ManagedLedgerOffloadRole();
            String roleSessionName = offloadPolicies.getS3ManagedLedgerOffloadRoleSessionName();
            return new STSAssumeRoleSessionCredentialsProvider.Builder(roleName, roleSessionName).build();
        }
    }

}
