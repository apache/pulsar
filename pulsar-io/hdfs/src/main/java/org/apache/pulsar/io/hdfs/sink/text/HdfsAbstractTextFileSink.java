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
package org.apache.pulsar.io.hdfs.sink.text;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.hdfs.sink.HdfsAbstractSink;

public abstract class HdfsAbstractTextFileSink<K, V> extends HdfsAbstractSink<K,V> implements Sink<V> {

	protected FSDataOutputStream hdfsStream;
	protected OutputStreamWriter writer;
	
	@Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
	   super.open(config, sinkContext);
	   writer = createWriter();
	}
	
	@Override
	public void close() throws Exception {
		writer.flush();
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
			e.printStackTrace();
			record.fail();
		}
	}
	
	@Override
	protected void openStream() throws IOException {
		openHdfsStream();
		createWriter();
	}
	
	@Override
	protected Syncable getStream() {
		return hdfsStream;
	}
	
	private void openHdfsStream() throws IOException {
		Path path = getPath();
		FileSystem fs = getFileSystemAsUser(getConfiguration(), getUserGroupInformation());
		hdfsStream = fs.exists(path) ? fs.append(path) : fs.create(path);
	}
	
	private OutputStreamWriter createWriter() throws IOException {	
		String encoding = StringUtils.isNotBlank(hdfsSinkConfig.getEncoding()) ? 
				hdfsSinkConfig.getEncoding() : Charset.defaultCharset().name();
		
		return new OutputStreamWriter(new BufferedOutputStream(hdfsStream), encoding);
	}
}
