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

import org.apache.commons.lang3.StringUtils;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Objects;

import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.v1.ReaderV1Impl;

/**
 *
 * @deprecated Use {@link PulsarClient#newReader()} to construct and configure a {@link Reader} instance
 */
@Deprecated
public class ReaderConfiguration implements Serializable {

    private final ReaderConfigurationData<byte[]> conf = new ReaderConfigurationData<>();

    private ReaderListener<byte[]> readerListener;

    /**
     * @return the configured {@link ReaderListener} for the reader
     */
    public ReaderListener<byte[]> getReaderListener() {
        return readerListener;
    }

    /**
     * Sets a {@link ReaderListener} for the reader
     * <p>
     * When a {@link ReaderListener} is set, application will receive messages through it. Calls to
     * {@link Reader#readNext()} will not be allowed.
     *
     * @param readerListener
     *            the listener object
     */
    public ReaderConfiguration setReaderListener(ReaderListener<byte[]> readerListener) {
        Objects.requireNonNull(readerListener);
        this.readerListener = readerListener;
        conf.setReaderListener(new org.apache.pulsar.shade.client.api.v2.ReaderListener<byte[]>() {

            @Override
            public void received(org.apache.pulsar.shade.client.api.v2.Reader<byte[]> v2Reader, Message<byte[]> msg) {
                readerListener.received(new ReaderV1Impl(v2Reader), msg);
            }

            @Override
            public void reachedEndOfTopic(org.apache.pulsar.shade.client.api.v2.Reader<byte[]> reader) {
                readerListener.reachedEndOfTopic(new ReaderV1Impl(reader));
            }
        });
        return this;
    }

    /**
     * @return the configure receiver queue size value
     */
    public int getReceiverQueueSize() {
        return conf.getReceiverQueueSize();
    }

    /**
     * @return the CryptoKeyReader
     */
    public CryptoKeyReader getCryptoKeyReader() {
        return conf.getCryptoKeyReader();
    }

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    public ReaderConfiguration setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        Objects.requireNonNull(cryptoKeyReader);
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified
     *
     * @param action
     *            The action to take when the decoding fails
     */
    public void setCryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
    }

    /**
     * @return The ConsumerCryptoFailureAction
     */
    public ConsumerCryptoFailureAction getCryptoFailureAction() {
        return conf.getCryptoFailureAction();
    }

    /**
     * Sets the size of the consumer receive queue.
     * <p>
     * The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     * </p>
     * Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     */
    public ReaderConfiguration setReceiverQueueSize(int receiverQueueSize) {
        checkArgument(receiverQueueSize >= 0, "Receiver queue size cannot be negative");
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    /**
     * @return the consumer name
     */
    public String getReaderName() {
        return conf.getReaderName();
    }

    /**
     * Set the consumer name.
     *
     * @param readerName
     */
    public ReaderConfiguration setReaderName(String readerName) {
        checkArgument(StringUtils.isNotBlank(readerName));
        conf.setReaderName(readerName);
        return this;
    }

    /**
     * @return the subscription role prefix for subscription auth
     */
    public String getSubscriptionRolePrefix() {
        return conf.getSubscriptionRolePrefix();
    }

    /**
     * Set the subscription role prefix for subscription auth. The default prefix is "reader".
     *
     * @param subscriptionRolePrefix
     */
    public ReaderConfiguration setSubscriptionRolePrefix(String subscriptionRolePrefix) {
        checkArgument(StringUtils.isNotBlank(subscriptionRolePrefix));
        conf.setSubscriptionRolePrefix(subscriptionRolePrefix);
        return this;
    }

    public ReaderConfigurationData<byte[]> getReaderConfigurationData() {
        return conf;
    }

    private static final long serialVersionUID = 1L;
}
