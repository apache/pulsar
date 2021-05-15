/*
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
package org.apache.pulsar.io.dis;

import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISClientAsync;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * @author shoothzj
 */
@Slf4j
public class DisSink implements Sink<byte[]> {

    private DISAsync disAsync;

    private String streamId;

    private String streamName;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        final DisConfig disSinkConfig = DisConfig.load(config);
        final DISConfig disConfig = new DISConfig();
        disConfig.setEndpoint(disSinkConfig.getDisEndpoint());
        disConfig.setAK(disSinkConfig.getAk());
        disConfig.setSK(disSinkConfig.getSk());
        disConfig.setProjectId(disSinkConfig.getProjectId());
        disConfig.setRegion(disSinkConfig.getRegion());
        disAsync = new DISClientAsync(disConfig);
        streamId = disSinkConfig.getStreamId();
        streamName = disSinkConfig.getStreamName();
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        final PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamId(streamId);
        putRecordsRequest.setStreamName(streamName);
        final PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
        if (record.getKey().isPresent()) {
            putRecordsRequestEntry.setPartitionKey(record.getKey().get());
        }
        putRecordsRequestEntry.setData(ByteBuffer.wrap(record.getValue()));
        putRecordsRequest.setRecords(Collections.singletonList(putRecordsRequestEntry));
        disAsync.putRecordsAsync(putRecordsRequest, new AsyncHandler<PutRecordsResult>() {
            @Override
            public void onError(Exception exception) throws Exception {
                log.error("send to dis error, error is ", exception);
                record.fail();
            }

            @Override
            public void onSuccess(PutRecordsResult putRecordsResult) throws Exception {
                record.ack();
            }
        });
    }

    @Override
    public void close() throws Exception {
        disAsync.close();
    }

}
