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
package org.apache.pulsar.io.hdfs.sink.seq;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;

/**
 * This Sink should be used when the records are originating from a sequential source,
 * and we want to retain the record sequence. 
 * 
 * This class uses the record's sequence id, as the sequence id in the HDFS Sequence File if
 * it is available, if not a sequence id is auto-generated for each new record.
 * 
 */
public class HdfsSequentialTextSink extends HdfsAbstractSequenceFileSink<Long, String, LongWritable, Text> {
	
	private AtomicLong counter;

	@Override
	public Writer getWriter() throws IOException {
		// Reset the counter
		counter = new AtomicLong(0);
	
		CompressionCodec codec = hdfsSinkConfig.getCompressionCodec() != null ? 
				hdfsSinkConfig.getCompressionCodec() : new DefaultCodec();
		
		return SequenceFile.createWriter(getConfiguration(), 
				                         Writer.appendIfExists(true),
				                         Writer.keyClass(LongWritable.class),
				                         Writer.valueClass(Text.class),
				                         Writer.compression(SequenceFile.CompressionType.BLOCK, codec),
				                         Writer.file(getPath()));
	}
	
	@Override
	public KeyValue<Long, String> extractKeyValue(Record<String> record) {
		Long sequence = record.getRecordSequence().orElseGet(() -> new Long(counter.incrementAndGet()));
        return new KeyValue<>(sequence, new String(record.getValue()));
	}
	
	@Override
	public KeyValue<LongWritable, Text> convert(KeyValue<Long, String> kv) {
		return new KeyValue<>(new LongWritable(kv.getKey()), new Text(kv.getValue()));
	}

}
