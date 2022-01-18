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
package org.apache.pulsar.io.hdfs2.sink.text;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.hdfs2.sink.HdfsAbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for HDFS Sinks that writes there contents to HDFS as Text Files.
 *
 * @param <K>
 * @param <V>
 */
public abstract class HdfsAbstractTextFileSink<K, V> extends HdfsAbstractSink<K, V> implements Sink<V> {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsAbstractTextFileSink.class);

    protected OutputStreamWriter writer;

    @Override
    protected void createWriter() throws IOException {
        writer = new OutputStreamWriter(new BufferedOutputStream(openHdfsStream()), getEncoding());
    }

    @Override
    public void close() throws Exception {
        writer.close();
        super.close();
    }

    @Override
    public void write(Record<V> record) {
       try {
           KeyValue<K, V> kv = extractKeyValue(record);
           writer.write(kv.getValue().toString());

           if (hdfsSinkConfig.getSeparator() != '\u0000') {
              writer.write(hdfsSinkConfig.getSeparator());
           }
           unackedRecords.put(record);
        } catch (IOException | InterruptedException e) {
            LOG.error("Unable to write to file " + getPath(), e);
            record.fail();
        }
    }

    private OutputStream openHdfsStream() throws IOException {
       if (hdfsSinkConfig.getCompression() != null) {
          return getCompressionCodec().createOutputStream(getHdfsStream());
       } else {
          return getHdfsStream();
       }
    }
}