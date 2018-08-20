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
package org.apache.pulsar.io.hdfs;

import java.io.Serializable;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public abstract class AbstractHdfsConfig implements Serializable {

	public static final String BZIP2 = "BZip2";
	public static final String DEFLATE = "Deflate";
	public static final String GZIP = "Gzip";
	public static final String LZ4 = "Lz4";
	public static final String SNAPPY = "Snappy";
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop
     * will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration.    
	 */
	protected String hdfsConfigResources;
	
	/**
	 * The HDFS directory from which files should be read from or written to
	 */
	protected String directory;
	
	/**
	 * The character encoding for the files, e.g. UTF-8, ASCII, etc.
	 */
	protected String encoding;
	
	/**
	 * The compression codec used to compress/de-compress the files on HDFS. 
	 */
	protected String compression;
	
	/**
	 * The Kerberos user principal account to use for authentication
	 */
	protected String kerberosUserPrincipal;
	
	/**
	 * The full pathname to the Kerberos keytab file to use for authentication.
	 */
	protected String keytab;
	
	public void validate() {
		if (StringUtils.isEmpty(hdfsConfigResources) || StringUtils.isEmpty(directory) )
			throw new IllegalArgumentException("Required property not set.");
		
		if (StringUtils.isNotEmpty(compression)) {
			if (!Stream.of(BZIP2, DEFLATE, GZIP, LZ4, SNAPPY).anyMatch(compression::equalsIgnoreCase)) {
				throw new IllegalArgumentException("Invalid Compression code specified. Valid values are 'BZip2', 'Deflate', 'Gzip', 'Lz4', or 'Snappy'");
			}
		}
	
		if ( (StringUtils.isNotEmpty(kerberosUserPrincipal) && StringUtils.isEmpty(keytab)) ||
			 (StringUtils.isEmpty(kerberosUserPrincipal) && StringUtils.isNotEmpty(keytab)) ) {
			throw new IllegalArgumentException("Values for both kerberosUserPrincipal & keytab are required.");
		}
	}
    
    public CompressionCodec getCompressionCodec() {
    	if (StringUtils.isBlank(compression)) 
    		return null;
    	
    	if (compression.equalsIgnoreCase(BZIP2))
    		return new BZip2Codec();
    	
    	if (compression.equalsIgnoreCase(DEFLATE))
    		return new DeflateCodec();
    	
    	if (compression.equalsIgnoreCase(GZIP))
    		return new GzipCodec();
    	
    	if (compression.equalsIgnoreCase(LZ4))
    		return new Lz4Codec();
    	
    	if (compression.equalsIgnoreCase(SNAPPY))
    		return new SnappyCodec();
    	
    	return null;
    }

	public String getHdfsConfigResources() {
		return hdfsConfigResources;
	}

	public void setHdfsConfigResources(String hdfsConfigResources) {
		this.hdfsConfigResources = hdfsConfigResources;
	}

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public String getKerberosUserPrincipal() {
		return kerberosUserPrincipal;
	}

	public void setKerberosUserPrincipal(String kerberosUserPrincipal) {
		this.kerberosUserPrincipal = kerberosUserPrincipal;
	}

	public String getKeytab() {
		return keytab;
	}

	public void setKeytab(String keytab) {
		this.keytab = keytab;
	}
	
	public String getCompression() {
		return compression;
	}

	public void setCompression(String compression) {
		this.compression = compression;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

}
