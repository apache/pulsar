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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * 
 * @see KinesisClientLibConfiguration 
 */
@Connector(
        name = "kinesis",
        type = IOType.SOURCE,
        help = "A source connector that copies messages from Kinesis to Pulsar",
        configClass = KinesisSourceConfig.class
    )
public class KinesisSource extends AbstractKinesisConnector implements Source<byte[]> {

    private LinkedBlockingQueue<KinesisRecord> queue;
    private KinesisSourceConfig kinesisSourceConfig;
    private KinesisClientLibConfiguration kinesisClientLibConfig;
    private IRecordProcessorFactory recordProcessorFactory;
    private String workerId;
    private Worker worker;

    @Override
    public void close() throws Exception {
        worker.shutdown();
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        this.kinesisSourceConfig = KinesisSourceConfig.load(config);
        
        checkArgument(isNotBlank(kinesisSourceConfig.getAwsKinesisStreamName()), "empty kinesis-stream name");
        checkArgument(isNotBlank(kinesisSourceConfig.getAwsEndpoint()) || 
                      isNotBlank(kinesisSourceConfig.getAwsRegion()), 
                     "Either the aws-end-point or aws-region must be set");
        checkArgument(isNotBlank(kinesisSourceConfig.getAwsCredentialPluginParam()), "empty aws-credential param");
        
        if (kinesisSourceConfig.getInitialPositionInStream() == InitialPositionInStream.AT_TIMESTAMP) {
            checkArgument((kinesisSourceConfig.getStartAtTime() != null),"Timestamp must be specified");
        }
        
        queue = new LinkedBlockingQueue<KinesisRecord> (kinesisSourceConfig.getReceiveQueueSize());
        workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        
        AWSCredentialsProvider credentialsProvider = createCredentialProvider(
                kinesisSourceConfig.getAwsCredentialPluginName(), 
                kinesisSourceConfig.getAwsCredentialPluginParam());
        
        kinesisClientLibConfig = 
                new KinesisClientLibConfiguration(kinesisSourceConfig.getApplicationName(),
                        kinesisSourceConfig.getAwsKinesisStreamName(),
                        credentialsProvider,
                        workerId)
                .withRegionName(kinesisSourceConfig.getAwsRegion())
                .withInitialPositionInStream(kinesisSourceConfig.getInitialPositionInStream());
        
        if (kinesisSourceConfig.getInitialPositionInStream() == InitialPositionInStream.AT_TIMESTAMP) {
           kinesisClientLibConfig.withTimestampAtInitialPositionInStream(kinesisSourceConfig.getStartAtTime());
        }
        
        recordProcessorFactory = new KinesisRecordProcessorFactory(queue, kinesisSourceConfig);
        
        worker = new Worker(recordProcessorFactory, kinesisClientLibConfig);
        worker.run();
    }

    @Override
    public KinesisRecord read() throws Exception {
        return queue.take();
    }

}
