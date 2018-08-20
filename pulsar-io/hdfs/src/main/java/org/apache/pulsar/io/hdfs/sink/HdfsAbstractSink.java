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
package org.apache.pulsar.io.hdfs.sink;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.hdfs.AbstractHdfsConnector;
import org.apache.pulsar.io.hdfs.HdfsResources;

public abstract class HdfsAbstractSink<K, V> extends AbstractHdfsConnector implements Sink<V> {

	protected Path hdfsPath;
	protected HdfsSinkConfig hdfsSinkConfig;
	protected BlockingQueue<Record<V>> unackedRecords;
	protected HdfsSyncThread<V> syncThread;
	
	public abstract KeyValue<K, V> extractKeyValue(Record<V> record);
	protected abstract void openStream() throws IOException;
	protected abstract Syncable getStream();
	
	@Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
	   hdfsSinkConfig = HdfsSinkConfig.load(config);
	   hdfsSinkConfig.validate();
	   unackedRecords = new LinkedBlockingQueue<Record<V>> (hdfsSinkConfig.maxPendingRecords);
	   connectToHdfs();
	   openStream();
	   launchSyncThread();
	}
	
	@Override
	public void close() throws Exception {
		syncThread.halt();
		syncThread.join(0);
		 
		synchronized(getStream()) {
		   if (getStream() != null) {
			   getStream().hsync();
		   }
		}
		
		getFileSystem().close(); 
	}
	
	protected void connectToHdfs() throws IOException {
	     try {
		   HdfsResources resources = hdfsResources.get();
		   if (resources.getConfiguration() == null) {
		   	  resources = this.resetHDFSResources(hdfsSinkConfig);
		  	  hdfsResources.set(resources);
		   }
		       
		   Path baseDirectory = new Path(hdfsSinkConfig.getDirectory());
		       
		   if (!getFileSystem().exists(baseDirectory)) {
			   getFileSystemAsUser(getConfiguration(), getUserGroupInformation()).mkdirs(baseDirectory);
		   } 
		    	   
		   // Change permissions to 777
		   FsPermission permission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
		   getFileSystemAsUser(getConfiguration(), getUserGroupInformation()).setPermission(baseDirectory, permission);		       
		       
	     } catch (IOException ex) {
	      	hdfsResources.set(new HdfsResources(null, null, null));
	        throw ex;
	     }
	}
	 
	protected Path getPath() {
		if (hdfsPath == null) {	
		   hdfsPath = new Path(FilenameUtils.concat(hdfsSinkConfig.getDirectory(), 
					hdfsSinkConfig.getFilenamePrefix() + "-" + System.currentTimeMillis() + 
					hdfsSinkConfig.getFileExtension()));
		}
		
		return hdfsPath;
	}
	
	protected void launchSyncThread() {
		 syncThread = new HdfsSyncThread<V>(getStream(), unackedRecords, hdfsSinkConfig.getSyncInterval());
		 syncThread.start();
	}
	
}
