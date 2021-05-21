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

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Interface that abstracts the method to encrypt/decrypt message for End to End Encryption.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MessageCrypto<MetadataT, BuilderT> {

    int IV_LEN = 12;

    /*
     * Encrypt data key using the public key(s) in the argument. <p> If more than one key name is specified, data key is
     * encrypted using each of those keys. If the public key is expired or changed, application is responsible to remove
     * the old key and add the new key <p>
     *
     * @param keyNames List of public keys to encrypt data key
     *
     * @param keyReader Implementation to read the key values
     *
     */
    void addPublicKeyCipher(Set<String> keyNames, CryptoKeyReader keyReader) throws CryptoException;


    /*
     * Remove a key <p> Remove the key identified by the keyName from the list of keys.<p>
     *
     * @param keyName Unique name to identify the key
     *
     * @return true if succeeded, false otherwise
     */
    boolean removeKeyCipher(String keyName);

    /**
     * Return the maximum for a given buffer to be encrypted or decrypted.
     *
     * This is meant to allow to pre-allocate a buffer with enough space to be passed as
     *
     * @param inputLen the length of the input buffer
     * @return the maximum size of the buffer to hold the encrypted/decrypted version of the input buffer
     */
    int getMaxOutputSize(int inputLen);

    /*
     * Encrypt the payload using the data key and update message metadata with the keyname & encrypted data key
     *
     * @param encKeys One or more public keys to encrypt data key
     * @param msgMetadata Message Metadata
     * @param payload Message which needs to be encrypted
     * @param outBuffer the buffer where to write the encrypted payload. The buffer needs to be have enough space
     *              to hold the encrypted value. Use #getMaxOutputSize method to discover the max size.
     *
     * @throws PulsarClientException if the encryption fails
     */
    void encrypt(Set<String> encKeys, CryptoKeyReader keyReader,
                       Supplier<BuilderT> messageMetadataBuilderSupplier,
                 ByteBuffer payload, ByteBuffer outBuffer) throws PulsarClientException;

    /*
     * Decrypt the payload using the data key. Keys used to encrypt data key can be retrieved from msgMetadata
     *
     * @param msgMetadata Message Metadata
     * @param payload Message which needs to be decrypted
     * @param keyReader KeyReader implementation to retrieve key value
     * @param outBuffer the buffer where to write the encrypted payload. The buffer needs to be have enough space
     *              to hold the encrypted value. Use #getMaxOutputSize method to discover the max size.
     *
     * @return true if success, false otherwise
     */
    boolean decrypt(Supplier<MetadataT> messageMetadataSupplier, ByteBuffer payload,
                 ByteBuffer outBuffer, CryptoKeyReader keyReader);
}
