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


import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class StreamsRecordProcessorFactory implements IRecordProcessorFactory {

    private final LinkedBlockingQueue<StreamsRecord> queue;
    private final DynamoDBSourceConfig config;
    
    public StreamsRecordProcessorFactory(LinkedBlockingQueue<StreamsRecord> queue,
                                         DynamoDBSourceConfig kinesisSourceConfig) {
        this.queue = queue;
        this.config = kinesisSourceConfig;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new StreamsRecordProcessor(queue, config);
    }
}
