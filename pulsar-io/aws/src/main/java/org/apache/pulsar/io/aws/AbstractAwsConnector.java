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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.utils.StringUtils;

@Slf4j
public abstract class AbstractAwsConnector {
    
    public static final String ACCESS_KEY_NAME = "accessKey";
    public static final String SECRET_KEY_NAME = "secretKey";

    public AwsCredentialProviderPlugin createCredentialProvider(String awsCredentialPluginName,
                                                                   String awsCredentialPluginParam) {
        if (StringUtils.isNotBlank(awsCredentialPluginName)) {
            return createCredentialProviderWithPlugin(awsCredentialPluginName, awsCredentialPluginParam);
        } else {
            return defaultCredentialProvider(awsCredentialPluginParam);
        }
    }

    /**
     * Creates a instance of credential provider which can return {@link AWSCredentials} or {@link BasicAWSCredentials}
     * based on IAM user/roles.
     *
     * @param pluginFQClassName
     * @param param
     * @return
     * @throws IllegalArgumentException
     */
    public static AwsCredentialProviderPlugin createCredentialProviderWithPlugin(String pluginFQClassName, String param)
            throws IllegalArgumentException {
        try {
            Class<?> clazz = Class.forName(pluginFQClassName);
            Constructor<?> ctor = clazz.getConstructor();
            final AwsCredentialProviderPlugin plugin = (AwsCredentialProviderPlugin) ctor.newInstance(new Object[] {});
            plugin.init(param);
            return plugin;
        } catch (Exception e) {
            log.error("Failed to initialize AwsCredentialProviderPlugin {}", pluginFQClassName, e);
            throw new IllegalArgumentException(
                    String.format("invalid authplugin name %s , failed to init %s", pluginFQClassName, e.getMessage()));
        }
    }
    
    /**
     * It creates a default credential provider which takes accessKey and secretKey form configuration and creates
     * {@link AWSCredentials}
     *
     * @param awsCredentialPluginParam
     * @return
     */
    public AwsCredentialProviderPlugin defaultCredentialProvider(String awsCredentialPluginParam) {
        Map<String, String> credentialMap = new Gson().fromJson(awsCredentialPluginParam,
                new TypeToken<Map<String, String>>() {
                }.getType());
        String accessKey = credentialMap.get(ACCESS_KEY_NAME);
        String secretKey = credentialMap.get(SECRET_KEY_NAME);
        if (!(StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey))) {
            throw new IllegalArgumentException(
                    String.format(
                            "Default %s and %s must be present into json-map if AwsCredentialProviderPlugin not provided",
                            ACCESS_KEY_NAME, SECRET_KEY_NAME)
            );
        }
        return new AwsCredentialProviderPlugin() {
            @Override
            public void init(String param) {
                // noop

            }

            @Override
            public AWSCredentialsProvider getCredentialProvider() {
                return defaultCredentialProvider(accessKey, secretKey);
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
    
    private AWSCredentialsProvider defaultCredentialProvider(String accessKey, String secretKey) {
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return accessKey;
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return secretKey;
                    }
                };
            }
            @Override
            public void refresh() {
                // no-op
            }
        };
    }
}
