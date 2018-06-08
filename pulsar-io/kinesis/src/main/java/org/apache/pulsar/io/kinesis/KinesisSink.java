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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.io.core.RecordContext;
import org.apache.pulsar.io.core.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration.ThreadingModel;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * A Kinesis sink
 */
public class KinesisSink implements Sink<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisSink.class);

    private KinesisProducer kinesisProducer;
    private KinesisSinkConfig kinesisSinkConfig;
    private String streamName;

    public static final String ACCESS_KEY_NAME = "accessKey";
    public static final String SECRET_KEY_NAME = "secretKey";

    @Override
    public void write(RecordContext inputRecordContext, byte[] value) throws Exception {
        final String partitionedKey = inputRecordContext.getPartitionId();
        ListenableFuture<UserRecordResult> addRecordResult = kinesisProducer.addUserRecord(this.streamName,
                partitionedKey, ByteBuffer.wrap(value));
        addCallback(addRecordResult,
                ProducerSendCallback.create(this.streamName, inputRecordContext, System.nanoTime()), directExecutor());
    }

    @Override
    public void close() throws IOException {
        if (kinesisProducer != null) {
            kinesisProducer.flush();
            kinesisProducer.destroy();
        }
        LOG.info("Kinesis sink stopped.");
    }

    @Override
    public void open(Map<String, Object> config) throws Exception {
        kinesisSinkConfig = KinesisSinkConfig.load(config);

        checkArgument(isNotBlank(kinesisSinkConfig.getAwsKinesisStreamName()), "empty kinesis-stream name");
        checkArgument(isNotBlank(kinesisSinkConfig.getAwsEndpoint()), "empty aws-end-point");
        checkArgument(isNotBlank(kinesisSinkConfig.getAwsRegion()), "empty aws region name");
        checkArgument(isNotBlank(kinesisSinkConfig.getAwsKinesisStreamName()), "empty kinesis stream name");
        checkArgument(isNotBlank(kinesisSinkConfig.getAwsCredentialPluginParam()), "empty aws-credential param");

        KinesisProducerConfiguration kinesisConfig = new KinesisProducerConfiguration();
        kinesisConfig.setKinesisEndpoint(kinesisSinkConfig.getAwsEndpoint());
        kinesisConfig.setRegion(kinesisSinkConfig.getAwsRegion());
        kinesisConfig.setThreadingModel(ThreadingModel.POOLED);
        kinesisConfig.setThreadPoolSize(4);
        kinesisConfig.setCollectionMaxCount(1);
        AWSCredentialsProvider credentialsProvider = createCredentialProvider(
                kinesisSinkConfig.getAwsCredentialPluginName(), kinesisSinkConfig.getAwsCredentialPluginParam());
        kinesisConfig.setCredentialsProvider(credentialsProvider);

        this.streamName = kinesisSinkConfig.getAwsKinesisStreamName();
        this.kinesisProducer = new KinesisProducer(kinesisConfig);

        LOG.info("Kinesis sink started. {}", (ReflectionToStringBuilder.toString(kinesisConfig, ToStringStyle.SHORT_PREFIX_STYLE)));
    }

    private AWSCredentialsProvider createCredentialProvider(String awsCredentialPluginName,
            String awsCredentialPluginParam) {
        if (isNotBlank(awsCredentialPluginName)) {
            return createCredentialProvider(awsCredentialPluginName, awsCredentialPluginParam);
        } else {
            return defaultCredentialProvider(awsCredentialPluginParam);            
        }
    }

    private static final class ProducerSendCallback implements FutureCallback<UserRecordResult> {

        private RecordContext resultContext;
        private String streamName;
        private long startTime = 0;
        private final Handle<ProducerSendCallback> recyclerHandle;

        private ProducerSendCallback(Handle<ProducerSendCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static ProducerSendCallback create(String streamName, RecordContext result, long startTime) {
            ProducerSendCallback sendCallback = RECYCLER.get();
            sendCallback.resultContext = result;
            sendCallback.streamName = streamName;
            sendCallback.startTime = startTime;
            return sendCallback;
        }

        private void recycle() {
            resultContext = null;
            streamName = null;
            startTime = 0;
            recyclerHandle.recycle(this);
        }

        private static final Recycler<ProducerSendCallback> RECYCLER = new Recycler<ProducerSendCallback>() {
            @Override
            protected ProducerSendCallback newObject(Handle<ProducerSendCallback> handle) {
                return new ProducerSendCallback(handle);
            }
        };

        @Override
        public void onSuccess(UserRecordResult result) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully published message for replicator of {}-{} with latency", this.streamName,
                        result.getShardId(), TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - startTime)));
            }
            this.resultContext.ack();
            recycle();
        }

        @Override
        public void onFailure(Throwable exception) {
            LOG.error("[{}] Failed to published message for replicator of {}-{} ", streamName,
                    resultContext.getPartitionId(), resultContext.getRecordSequence());
            this.resultContext.fail();
            recycle();
        }
    }

    public static AWSCredentialsProvider createCredentialProviderPlugin(String pluginFQClassName, String param)
            throws IllegalArgumentException {
        try {
            Class<?> clazz = Class.forName(pluginFQClassName);
            Constructor<?> ctor = clazz.getConstructor();
            final AwsCredentialProviderPlugin plugin = (AwsCredentialProviderPlugin) ctor.newInstance(new Object[] {});
            plugin.init(param);
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new AWSCredentials() {
                        @Override
                        public String getAWSAccessKeyId() {
                            return plugin.getAWSAccessKeyId();
                        }

                        @Override
                        public String getAWSSecretKey() {
                            return plugin.getAWSSecretKey();
                        }
                    };
                }
                @Override
                public void refresh() {
                    plugin.refresh();
                }
            };
        } catch (Exception e) {
            LOG.error("Failed to initialize AwsCredentialProviderPlugin {}", pluginFQClassName, e);
            throw new IllegalArgumentException(
                    String.format("invalid authplugin name %s , failed to init %s", pluginFQClassName, e.getMessage()));
        }
    }

    private AWSCredentialsProvider defaultCredentialProvider(String awsCredentialPluginParam) {
        String[] credentials = awsCredentialPluginParam.split(",");
        String accessKey = null;
        String secretKey = null;
        if (credentials.length == 2) {
            for (String credential : credentials) {
                String[] keys = credential.split("=");
                if (keys.length == 2) {
                    if (keys[0].equals(ACCESS_KEY_NAME)) {
                        accessKey = keys[1];
                    } else if (keys[0].equals(SECRET_KEY_NAME)) {
                        secretKey = keys[1];
                    }
                }
            }
        }
        checkArgument(isNotBlank(accessKey) && isNotBlank(secretKey),
                String.format("access-key/secret-key not present in param: format: %s=<access-key>,%s=<secret-key>",
                        ACCESS_KEY_NAME, SECRET_KEY_NAME));
        return defaultCredentialProvider(accessKey, secretKey);
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