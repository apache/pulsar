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
package org.apache.pulsar.io.hdfs2.sink.seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.hdfs2.sink.HdfsAbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS Sink that writes it contents to HDFS as Sequence Files.
 *
 * @param <K> - The incoming Key type
 * @param <V> - The incoming Value type
 * @param <HdfsK> - The HDFS Key type
 * @param <HdfsV> - The HDFS Value type
 */
public abstract class HdfsAbstractSequenceFileSink<K, V, HdfsK, HdfsV>
    extends HdfsAbstractSink<K, V> implements Sink<V> {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsAbstractSequenceFileSink.class);

    protected AtomicLong counter;
    protected FSDataOutputStream hdfsStream;
    protected Writer writer = null;

    public abstract KeyValue<HdfsK, HdfsV> convert(KeyValue<K, V> kv);

    @Override
    public void close() throws Exception {
       writer.close();
       super.close();
    }

    @Override
    protected void createWriter() throws IOException {
       writer = getWriter();
    }

    @Override
    public void write(Record<V> record) {
       try {
            KeyValue<K, V> kv = extractKeyValue(record);
            KeyValue<HdfsK, HdfsV> keyValue = convert(kv);
            writer.append(keyValue.getKey(), keyValue.getValue());
            unackedRecords.put(record);
        } catch (IOException | InterruptedException e) {
            LOG.error("Unable to write to file " + getPath(), e);
            record.fail();
        }
    }

    protected Writer getWriter() throws IOException {
        counter = new AtomicLong(0);
        List<Option> options = getOptions();
        return SequenceFile.createWriter(getConfiguration(),
                options.toArray(new Option[options.size()]));
     }

    protected List<Option> getOptions() throws IllegalArgumentException, IOException {
        List<Option> list = new ArrayList<Option>();
        list.add(Writer.stream(getHdfsStream()));

        if (getCompressionCodec() != null) {
            list.add(Writer.compression(SequenceFile.CompressionType.RECORD, getCompressionCodec()));
        }
        return list;
    }
}
