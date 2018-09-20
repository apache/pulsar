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
package org.apache.bookkeeper.mledger.offload.jcloud.config;

import java.io.Serializable;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.jclouds.domain.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for tiered storage.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public abstract class JCloudBlobStoreConfiguration implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public static final int MB = 1024 * 1024;

    protected static final Logger LOG = LoggerFactory.getLogger(JCloudBlobStoreConfiguration.class);

    // Driver to use to off-load old data to long term storage
    protected String managedLedgerOffloadDriver = null;

    // Maximum number of thread pool threads for ledger off-loading
    protected int managedLedgerOffloadMaxThreads = 2;

    // Max block size in bytes.
    protected int maxBlockSizeInBytes = 64 * MB; // 64MB

    // Read buffer size in bytes.
    protected int readBufferSizeInBytes = MB; // 1MB

    protected JCloudBlobStoreProvider provider;

    public abstract String getRegion();

    public abstract String getBucket();

    public abstract String getServiceEndpoint();

    public abstract Credentials getCredentials();

    public abstract void validate();
}