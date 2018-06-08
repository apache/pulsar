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

package org.apache.pulsar.io.kinesis;

/**
 * Kinesis source/sink calls credential-provider while refreshing aws accessKey and secreKey. So, implementation
 * AwsCredentialProviderPlugin needs to makes sure to return non-expired keys when it requires.
 *
 */
public interface AwsCredentialProviderPlugin {
    
    /**
     * accepts aws-account related param and initialize credential provider.
     *  
     * @param param
     */
    void init(String param);
    
    /**
     * Returns the AWS access key ID for this credentials object.
     * 
     * @return The AWS access key ID for this credentials object.
     */
    String getAWSAccessKeyId();

    /**
     * Returns the AWS secret access key for this credentials object.
     * 
     * @return The AWS secret access key for this credentials object.
     */
    String getAWSSecretKey();
    
    /**
     * Forces this credentials provider to refresh its credentials. For many
     * implementations of credentials provider, this method may simply be a
     * no-op, such as any credentials provider implementation that vends
     * static/non-changing credentials. For other implementations that vend
     * different credentials through out their lifetime, this method should
     * force the credentials provider to refresh its credentials.
     */
    void refresh();

}
