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

package org.apache.pulsar.client.api;

public enum ConsumerCryptoFailureAction {
    FAIL, // This is the default option to fail consume until crypto succeeds
    DISCARD, // Message is silently acknowledged and not delivered to the application
    /**
     * 
     * <pre>
     * Deliver the encrypted message to the application. It's the application's responsibility to decrypt the message.
     * If message is also compressed, decompression will fail. If message contain batch messages, client will not be
     * able to retrieve individual messages in the batch.
     * </pre>
     * 
     * Delivered encrypted message will contain encrypted payload along with properties which can be used to uncompress
     * and decrypt the payload. Message will contain following properties to decrypt message:
     * 
     * <ul>
     * <li>{@value #PULSAR_ENCRYPTION_KEY_PROP}: Encryption keys in json format of {@link EncryptionKeyInfo}</li>
     * <li>{@value #PULSAR_ENCRYPTION_PARAM_BASE64_ENCODED_PROP} : encryption param required to decrypt message</li>
     * <li>{@value #PULSAR_ENCRYPTION_ALGO_PROP}: encryption algorithm</li>
     * <li>{@value #PULSAR_COMPRESSION_TYPE_PROP}: compression type if message is already compressed
     * {@link CompressionType} (null if message is not compressed).</li>
     * <li>{@value #PULSAR_UNCOMPRESSED_MSG_SIZE_PROP}: uncompressed message size (null if message is not compressed).
     * </li>
     * <li>{@value #PULSAR_BATCH_SIZE_PROP}: number of messages present into batch message (null if message is not batch
     * message).</li>
     * </ul>
     * 
     * 
     */
    CONSUME;
    
    public static final String PULSAR_ENCRYPTION_KEY_PROP = "__pulsar_encryption_key__";
    public static final String PULSAR_ENCRYPTION_PARAM_BASE64_ENCODED_PROP = "__pulsar_encryption_param_base64_encoded__";
    public static final String PULSAR_ENCRYPTION_ALGO_PROP = "__pulsar_encryption_algo__";
    public static final String PULSAR_COMPRESSION_TYPE_PROP = "__pulsar_compression_type__";
    public static final String PULSAR_UNCOMPRESSED_MSG_SIZE_PROP = "__pulsar_uncompressed_msg_size__";
    public static final String PULSAR_BATCH_SIZE_PROP = "__pulsar_batch_size__";
}
