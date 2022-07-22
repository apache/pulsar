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
package org.apache.pulsar.common.policies.data;

import lombok.Data;

@Data
public class TxnBufferedWriterStat {

    /** Whether to enable the batch feature when writing to Bookie. **/
    private boolean batchEnabled = false;

    /** If enabled the feature-batch, this attribute means maximum log records count in a batch. **/
    private int batchedWriteMaxRecords = 512;

    /** If enabled the feature-batch, this attribute means bytes size in a batch. **/
    private int batchedWriteMaxSize;

    /**
     * If enabled the feature-batch, this attribute means maximum wait time(in millis) for the first record in a batch.
     */
    private int batchedWriteMaxDelayInMillis;
}
