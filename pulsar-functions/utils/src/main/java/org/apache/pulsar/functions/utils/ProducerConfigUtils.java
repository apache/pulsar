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
