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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.functions.proto.Function;

public class ProducerConfigUtils {
    public static Function.ProducerSpec convert(ProducerConfig conf) {
        Function.ProducerSpec.Builder pbldr = Function.ProducerSpec.newBuilder();
        if (conf.getMaxPendingMessages() != null) {
            pbldr.setMaxPendingMessages(conf.getMaxPendingMessages());
        }
        if (conf.getMaxPendingMessagesAcrossPartitions() != null) {
            pbldr.setMaxPendingMessagesAcrossPartitions(conf.getMaxPendingMessagesAcrossPartitions());
        }
        if (conf.getUseThreadLocalProducers() != null) {
            pbldr.setUseThreadLocalProducers(conf.getUseThreadLocalProducers());
        }
        if (conf.getBatchBuilder() != null) {
            pbldr.setBatchBuilder(conf.getBatchBuilder());
        }

        return pbldr.build();
    }

    public static ProducerConfig convertFromSpec(Function.ProducerSpec spec) {
        ProducerConfig producerConfig = new ProducerConfig();
        if (spec.getMaxPendingMessages() != 0) {
            producerConfig.setMaxPendingMessages(spec.getMaxPendingMessages());
        }
        if (spec.getMaxPendingMessagesAcrossPartitions() != 0) {
            producerConfig.setMaxPendingMessagesAcrossPartitions(spec.getMaxPendingMessagesAcrossPartitions());
        }
        if (spec.getBatchBuilder() != null) {
            producerConfig.setBatchBuilder(spec.getBatchBuilder());
        }
        producerConfig.setUseThreadLocalProducers(spec.getUseThreadLocalProducers());
        return producerConfig;
    }
}
