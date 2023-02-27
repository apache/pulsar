/*
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
package org.apache.pulsar.broker.validator;

import org.apache.pulsar.broker.ServiceConfiguration;

public class TransactionBatchedWriteValidator {

    public static void validate(ServiceConfiguration configuration){
        if (configuration.isTransactionPendingAckBatchedWriteEnabled()){
            if (configuration.getTransactionPendingAckBatchedWriteMaxRecords() < 10){
                throw new IllegalArgumentException("Configuration field "
                        + "'transactionPendingAckBatchedWriteMaxRecords' value must be greater than or equal to 10");
            }
            if (configuration.getTransactionPendingAckBatchedWriteMaxSize() < 1024 * 128){
                throw new IllegalArgumentException("Configuration field "
                        + "'transactionPendingAckBatchedWriteMaxSize' value must be greater than or equal to 128k");
            }
            if (configuration.getTransactionPendingAckBatchedWriteMaxDelayInMillis() < 1){
                throw new IllegalArgumentException("Configuration field "
                        + "'transactionPendingAckBatchedWriteMaxDelayInMillis' value must be greater than or equal to"
                        + " 1");
            }
        }
        if (configuration.isTransactionLogBatchedWriteEnabled()){
            if (configuration.getTransactionLogBatchedWriteMaxRecords() < 10){
                throw new IllegalArgumentException("Configuration field "
                        + "'transactionLogBatchedWriteMaxRecords' value must be greater than or equal to 10");
            }
            if (configuration.getTransactionLogBatchedWriteMaxSize() < 1024 * 128){
                throw new IllegalArgumentException("Configuration field "
                        + "'transactionLogBatchedWriteMaxSize' value must be greater than or equal to 128k");
            }
            if (configuration.getTransactionLogBatchedWriteMaxDelayInMillis() < 1){
                throw new IllegalArgumentException("Configuration field "
                        + "'transactionLogBatchedWriteMaxDelayInMillis' value must be greater than or equal to 1");
            }
        }
    }
}
