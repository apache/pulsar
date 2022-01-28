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
package org.apache.pulsar.io.hbase.sink;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.source.PulsarSourceConfig;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.hbase.TableUtils;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * hbase Sink test
 */
@Slf4j
public class HbaseGenericRecordSinkTest {
    private Message<GenericRecord> message;


    /**
     * A Simple class to test hbase class
     */
    @Data
    public static class Foo {
        private String rowKey;
        private String name;
        private String address;
        private int age;
        private boolean flag;
    }

    private String rowKeyName = "rowKey";
    private String familyName = "info";
    private String name = "name";
    private String address = "address";
    private String age = "age";
    private String flag = "flag";
    @Mock
    protected SinkContext mockSinkContext;

    @Test(enabled = false)
    public void TestOpenAndWriteSink() throws Exception {
        message = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;

        Map<String, Object> map = new HashMap<>();
        map.put("zookeeperQuorum", "localhost");
        map.put("zookeeperClientPort", "2181");
        map.put("zookeeperZnodeParent", "/hbase");
        map.put("tableName", "default:pulsar_hbase");
        map.put("rowKeyName", rowKeyName);
        map.put("familyName", familyName);

        List<String> qualifierNames = new ArrayList<>();
        qualifierNames.add(name);
        qualifierNames.add(address);
        qualifierNames.add(age);
        qualifierNames.add(flag);
        map.put("qualifierNames",qualifierNames);

        mockSinkContext = mock(SinkContext.class);
        HbaseGenericRecordSink sink = new HbaseGenericRecordSink();

        // prepare a foo Record
        Foo obj = new Foo();
        obj.setRowKey("rowKey_value");
        obj.setName("name_value");
        obj.setAddress("address_value");
        obj.setAge(30);
        obj.setFlag(true);
        AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        byte[] bytes = schema.encode(obj);
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(GenericSchemaImpl.of(schema.getSchemaInfo()));

        PulsarSourceConfig pulsarSourceConfig = mock(PulsarSourceConfig.class);
        Consumer consumer = mock(Consumer.class);

        Record<GenericRecord> record = PulsarRecord.<GenericRecord>builder()
            .message(message)
            .topicName("fake_topic_name")
            .ackFunction(() -> {
                if (pulsarSourceConfig
                        .getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                    consumer.acknowledgeCumulativeAsync(message);
                } else {
                    consumer.acknowledgeAsync(message);
                }
            }).failFunction(() -> {
                if (pulsarSourceConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                    throw new RuntimeException("Failed to process message: " + message.getMessageId());
                }
            })
            .build();

        genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());

        when(message.getValue())
                .thenReturn(genericAvroSchema.decode(bytes));

        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                obj.toString(),
                message.getValue().toString(),
                record.getValue().toString());

        // change batchSize to 1, to flush on each write.
        map.put("batchTimeMs", 1);
        map.put("batchSize", 1);
        // open should success
        sink.open(map,mockSinkContext);

        // write should success.
        sink.write(record);
        log.info("executed write");
        // sleep to wait backend flush complete
        Thread.sleep(500);

        // value has been written to hbase table, read it out and verify.
        Table table = TableUtils.getTable(map);
        Get scan = new Get(Bytes.toBytes(obj.getRowKey()));
        Result result = table.get(scan);
        byte[] byteName = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(name));
        byte[] byteAddress = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(address));
        byte[] byteAge = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(age));
        byte[] byteFlag = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(flag));
        Assert.assertEquals(obj.getName(), Bytes.toString(byteName));
        Assert.assertEquals(obj.getAddress(), Bytes.toString(byteAddress));
        Assert.assertEquals(obj.getAge(), Bytes.toInt(byteAge));
        Assert.assertEquals(obj.isFlag(), Bytes.toBoolean(byteFlag));

        table.close();
        sink.close();
    }

}
