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
package org.apache.pulsar.io.hdfs3.sink;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.io.hdfs3.Compression;
import org.apache.pulsar.io.hdfs3.sink.HdfsSinkConfig;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;


public class HdfsSinkConfigTests {
	
	@Test
	public final void loadFromYamlFileTest() throws IOException {
		File yamlFile = getFile("sinkConfig.yaml");
		HdfsSinkConfig config = HdfsSinkConfig.load(yamlFile.getAbsolutePath());
		assertNotNull(config);
		assertEquals("core-site.xml", config.getHdfsConfigResources());
		assertEquals("/foo/bar", config.getDirectory());
		assertEquals("prefix", config.getFilenamePrefix());
		assertEquals(Compression.SNAPPY, config.getCompression());
	}
	
	@Test
	public final void loadFromMapTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		map.put("directory", "/foo/bar");
		map.put("filenamePrefix", "prefix");
		map.put("compression", "SNAPPY");
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		assertNotNull(config);
		assertEquals("core-site.xml", config.getHdfsConfigResources());
		assertEquals("/foo/bar", config.getDirectory());
		assertEquals("prefix", config.getFilenamePrefix());
		assertEquals(Compression.SNAPPY, config.getCompression());
	}
	
	@Test
	public final void validValidateTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		map.put("directory", "/foo/bar");
		map.put("filenamePrefix", "prefix");
		map.put("fileExtension", ".txt");
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class, 
			expectedExceptionsMessageRegExp = "Required property not set.")
	public final void missingDirectoryValidateTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class, 
		  expectedExceptionsMessageRegExp = "Required property not set.")
	public final void missingHdfsConfigsValidateTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("directory", "/foo/bar");
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	@Test(expectedExceptions = InvalidFormatException.class)
	public final void invalidCodecValidateTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		map.put("directory", "/foo/bar");
		map.put("filenamePrefix", "prefix");
		map.put("fileExtension", ".txt");
		map.put("compression", "bad value");
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class, 
		  expectedExceptionsMessageRegExp = "Sync Interval cannot be negative")
	public final void invalidSyncIntervalTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		map.put("directory", "/foo/bar");
		map.put("filenamePrefix", "prefix");
		map.put("fileExtension", ".txt");
		map.put("syncInterval", -1);
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class, 
		  expectedExceptionsMessageRegExp = "Max Pending Records must be a positive integer")
	public final void invalidMaxPendingRecordsTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		map.put("directory", "/foo/bar");
		map.put("filenamePrefix", "prefix");
		map.put("fileExtension", ".txt");
		map.put("maxPendingRecords", 0);
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class, 
		  expectedExceptionsMessageRegExp = "Values for both kerberosUserPrincipal & keytab are required.")
	public final void kerberosValidateTest() throws IOException {
		Map<String, Object> map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "core-site.xml");
		map.put("directory", "/foo/bar");
		map.put("filenamePrefix", "prefix");
		map.put("keytab", "/etc/keytab/hdfs.client.ktab");
		
		HdfsSinkConfig config = HdfsSinkConfig.load(map);
		config.validate();
	}
	
	private File getFile(String name) {
		ClassLoader classLoader = getClass().getClassLoader();
		return new File(classLoader.getResource(name).getFile());
	}
}
