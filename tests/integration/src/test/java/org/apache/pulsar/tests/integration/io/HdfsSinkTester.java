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
package org.apache.pulsar.tests.integration.io;

import java.util.Map;

import org.apache.pulsar.tests.integration.containers.HdfsContainer;
import org.testcontainers.containers.GenericContainer;

import static com.google.common.base.Preconditions.checkState;

public class HdfsSinkTester extends SinkTester {
	
	private static final String NAME = "HDFS";
	
	private HdfsContainer hdfsCluster;
	
	public HdfsSinkTester() {
		super(SinkType.HDFS);
		
		// TODO How do I get the core-site.xml, and hdfs-site.xml files from the container?
		sinkConfig.put("hdfsConfigResources", "");
		sinkConfig.put("directory", "/testing/test");
	}

	@Override
	public void findSinkServiceContainer(Map<String, GenericContainer<?>> containers) {
		GenericContainer<?> container = containers.get(NAME);	
		checkState(container instanceof HdfsContainer, "No HDFS service found in the cluster");
	    this.hdfsCluster = (HdfsContainer) container;
	}

	@Override
	public void prepareSink() throws Exception {
		// Create the test directory
		hdfsCluster.execInContainer("/hadoop/bin/hdfs","dfs", "-mkdir", "/tmp/testing");
		hdfsCluster.execInContainer("/hadoop/bin/hdfs", "-chown", "tester:testing", "/tmp/testing");
		
		// Execute all future commands as the "tester" user
		hdfsCluster.execInContainer("export HADOOP_USER_NAME=tester");
	}

	@Override
	public void validateSinkResult(Map<String, String> kvs) {
		// TODO Auto-generated method stub

	}

}
