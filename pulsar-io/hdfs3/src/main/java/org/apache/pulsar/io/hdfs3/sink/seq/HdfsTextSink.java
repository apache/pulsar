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
package org.apache.pulsar.io.hdfs3.sink.seq;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;

/**
 * A Simple Sink class for Hdfs Sequence File.
 */
public class HdfsTextSink extends
     HdfsAbstractSequenceFileSink<String, String, Text, Text> {

    @Override
    protected List<Option> getOptions() throws IllegalArgumentException, IOException {
        List<Option> opts = super.getOptions();
        opts.add(Writer.keyClass(Text.class));
        opts.add(Writer.valueClass(Text.class));
        return opts;
    }

    @Override
    public KeyValue<String, String> extractKeyValue(Record<String> record) {
       String key = record.getKey().orElseGet(() -> record.getValue());
       return new KeyValue<>(key, record.getValue());
    }

    @Override
    public KeyValue<Text, Text> convert(KeyValue<String, String> kv) {
       return new KeyValue<>(new Text(kv.getKey()), new Text(kv.getValue()));
    }
}
