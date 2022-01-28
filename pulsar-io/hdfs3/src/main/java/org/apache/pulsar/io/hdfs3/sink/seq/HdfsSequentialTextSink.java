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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;

/**
 * This Sink should be used when the records are originating from a sequential source,
 * and we want to retain the record sequence.This class uses the record's sequence id as
 * the sequence id in the HDFS Sequence File if it is available, if not a sequence id is
 * auto-generated for each new record.
 */
public class HdfsSequentialTextSink extends HdfsAbstractSequenceFileSink<Long, String, LongWritable, Text> {

    private AtomicLong counter;

    @Override
    public Writer getWriter() throws IOException {
       counter = new AtomicLong(0);

       return SequenceFile
                .createWriter(
                   getConfiguration(),
                   getOptions().toArray(new Option[getOptions().size()]));
    }

    @Override
    protected List<Option> getOptions() throws IllegalArgumentException, IOException {
        List<Option> opts = super.getOptions();
        opts.add(Writer.keyClass(LongWritable.class));
        opts.add(Writer.valueClass(Text.class));
        return opts;
    }

    @Override
    public KeyValue<Long, String> extractKeyValue(Record<String> record) {
       Long sequence = record.getRecordSequence().orElseGet(() -> counter.incrementAndGet());
       return new KeyValue<>(sequence, record.getValue());
    }

    @Override
    public KeyValue<LongWritable, Text> convert(KeyValue<Long, String> kv) {
       return new KeyValue<>(new LongWritable(kv.getKey()), new Text(kv.getValue()));
    }
}
