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
package org.apache.pulsar.io.hdfs3;

import java.io.Serializable;

import lombok.Data;
import lombok.experimental.Accessors;

import org.apache.commons.lang.StringUtils;

/**
 * Configuration object for all HDFS components.
 */
@Data
@Accessors(chain = true)
public abstract class AbstractHdfsConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A file or comma separated list of files which contains the Hadoop file system configuration,
     * e.g. 'core-site.xml', 'hdfs-site.xml'.
     */
    private String hdfsConfigResources;

    /**
     * The HDFS directory from which files should be read from or written to.
     */
    private String directory;

    /**
     * The character encoding for the files, e.g. UTF-8, ASCII, etc.
     */
    private String encoding;

    /**
     * The compression codec used to compress/de-compress the files on HDFS.
     */
    private Compression compression;

    /**
     * The Kerberos user principal account to use for authentication.
     */
    private String kerberosUserPrincipal;

    /**
     * The full pathname to the Kerberos keytab file to use for authentication.
     */
    private String keytab;

    public void validate() {
        if (StringUtils.isEmpty(hdfsConfigResources) || StringUtils.isEmpty(directory)) {
           throw new IllegalArgumentException("Required property not set.");
        }

        if ((StringUtils.isNotEmpty(kerberosUserPrincipal) && StringUtils.isEmpty(keytab))
            || (StringUtils.isEmpty(kerberosUserPrincipal) && StringUtils.isNotEmpty(keytab))) {
          throw new IllegalArgumentException("Values for both kerberosUserPrincipal & keytab are required.");
        }
    }
}
