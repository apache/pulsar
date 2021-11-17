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
package org.apache.pulsar.io.dynamodb;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "dynamodb",
        type = IOType.SOURCE,
        help = "A source connector that copies messages from DynamoDB Streams to Pulsar",
        configClass = DynamoDBSourceConfig.class
    )
@Slf4j
public class DynamoDBSource extends AbstractAwsConnector implements Source<byte[]> {

    private LinkedBlockingQueue<StreamsRecord> queue;
    private DynamoDBSourceConfig dynamodbSourceConfig;
    private KinesisClientLibConfiguration kinesisClientLibConfig;
    private IRecordProcessorFactory recordProcessorFactory;
    private String workerId;
    private Worker worker;
    private Thread workerThread;
    private Throwable threadEx;


    @Override
    public void close() throws Exception {
        worker.shutdown();
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        this.dynamodbSourceConfig = DynamoDBSourceConfig.load(config);
        
        checkArgument(isNotBlank(dynamodbSourceConfig.getAwsDynamodbStreamArn()), "empty dynamo-stream arn");
//       Even if the endpoint is set, it seems to require a region to go with it
        checkArgument(isNotBlank(dynamodbSourceConfig.getAwsRegion()),
                     "The aws-region must be set");
        checkArgument(isNotBlank(dynamodbSourceConfig.getAwsCredentialPluginParam()), "empty aws-credential param");
        
        if (dynamodbSourceConfig.getInitialPositionInStream() == InitialPositionInStream.AT_TIMESTAMP) {
            checkArgument((dynamodbSourceConfig.getStartAtTime() != null),"Timestamp must be specified");
        }
        
        queue = new LinkedBlockingQueue<> (dynamodbSourceConfig.getReceiveQueueSize());
        workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        
        AwsCredentialProviderPlugin credentialsProvider = createCredentialProvider(
                dynamodbSourceConfig.getAwsCredentialPluginName(),
                dynamodbSourceConfig.getAwsCredentialPluginParam());

        AmazonDynamoDBStreams dynamoDBStreamsClient = dynamodbSourceConfig.buildDynamoDBStreamsClient(credentialsProvider);
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreamsClient);
        recordProcessorFactory = new StreamsRecordProcessorFactory(queue, dynamodbSourceConfig);

        kinesisClientLibConfig = new KinesisClientLibConfiguration(dynamodbSourceConfig.getApplicationName(),
                dynamodbSourceConfig.getAwsDynamodbStreamArn(),
                credentialsProvider.getCredentialProvider(),
                workerId)
                .withRegionName(dynamodbSourceConfig.getAwsRegion())
                .withInitialPositionInStream(dynamodbSourceConfig.getInitialPositionInStream());

        if(kinesisClientLibConfig.getInitialPositionInStream() == InitialPositionInStream.AT_TIMESTAMP) {
            kinesisClientLibConfig.withTimestampAtInitialPositionInStream(dynamodbSourceConfig.getStartAtTime());
        }

        worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(recordProcessorFactory,
                kinesisClientLibConfig,
                adapterClient,
                dynamodbSourceConfig.buildDynamoDBClient(credentialsProvider),
                dynamodbSourceConfig.buildCloudwatchClient(credentialsProvider));

        workerThread = new Thread(worker);
        workerThread.setDaemon(true);
        threadEx = null;
        workerThread.setUncaughtExceptionHandler((t, ex) -> {
            threadEx = ex;
            log.error("Worker died with error", ex);
        });
        workerThread.start();
    }

    @Override
    public StreamsRecord read() throws Exception {
        try {
            return queue.take();
        } catch (InterruptedException ex) {
            log.warn("Got interrupted when trying to fetch out of the queue");
            if (threadEx != null) {
                log.error("error from scheduler", threadEx);
            }
            throw ex;
        }
    }

}
