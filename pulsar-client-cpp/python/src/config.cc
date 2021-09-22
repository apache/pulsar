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
#include "utils.h"
#include <pulsar/ConsoleLoggerFactory.h>
#include "lib/Utils.h"
#include <memory>

template<typename T>
struct ListenerWrapper {
    PyObject* _pyListener;

    ListenerWrapper(py::object pyListener) :
        _pyListener(pyListener.ptr()) {
        Py_XINCREF(_pyListener);
    }

    ListenerWrapper(const ListenerWrapper& other) {
        _pyListener = other._pyListener;
        Py_XINCREF(_pyListener);
    }

    ListenerWrapper& operator=(const ListenerWrapper& other) {
        _pyListener = other._pyListener;
        Py_XINCREF(_pyListener);
        return *this;
    }

    virtual ~ListenerWrapper() {
        Py_XDECREF(_pyListener);
    }

    void operator()(T consumer, const Message& msg) {
        PyGILState_STATE state = PyGILState_Ensure();

        try {
            py::call<void>(_pyListener, py::object(&consumer), py::object(&msg));
        } catch (const py::error_already_set& e) {
            PyErr_Print();
        }

        PyGILState_Release(state);
    }
};

static ConsumerConfiguration& ConsumerConfiguration_setMessageListener(ConsumerConfiguration& conf,
                                                                       py::object pyListener) {
    conf.setMessageListener(ListenerWrapper<Consumer>(pyListener));
    return conf;
}

static ReaderConfiguration& ReaderConfiguration_setReaderListener(ReaderConfiguration& conf,
                                                                   py::object pyListener) {
    conf.setReaderListener(ListenerWrapper<Reader>(pyListener));
    return conf;
}

static ClientConfiguration& ClientConfiguration_setAuthentication(ClientConfiguration& conf,
                                                                  py::object authentication) {
    AuthenticationWrapper wrapper = py::extract<AuthenticationWrapper>(authentication);
    conf.setAuth(wrapper.auth);
    return conf;
}

static ConsumerConfiguration& ConsumerConfiguration_setCryptoKeyReader(ConsumerConfiguration& conf,
                                                                        py::object cryptoKeyReader) {
    CryptoKeyReaderWrapper cryptoKeyReaderWrapper = py::extract<CryptoKeyReaderWrapper>(cryptoKeyReader);
    conf.setCryptoKeyReader(cryptoKeyReaderWrapper.cryptoKeyReader);
    return conf;
}

static ProducerConfiguration& ProducerConfiguration_setCryptoKeyReader(ProducerConfiguration& conf,
                                                                        py::object cryptoKeyReader) {
    CryptoKeyReaderWrapper cryptoKeyReaderWrapper = py::extract<CryptoKeyReaderWrapper>(cryptoKeyReader);
    conf.setCryptoKeyReader(cryptoKeyReaderWrapper.cryptoKeyReader);
    return conf;
}

static ReaderConfiguration& ReaderConfiguration_setCryptoKeyReader(ReaderConfiguration& conf,
                                                                        py::object cryptoKeyReader) {
    CryptoKeyReaderWrapper cryptoKeyReaderWrapper = py::extract<CryptoKeyReaderWrapper>(cryptoKeyReader);
    conf.setCryptoKeyReader(cryptoKeyReaderWrapper.cryptoKeyReader);
    return conf;
}

class LoggerWrapper: public Logger {
    PyObject* const _pyLogger;
    const int _pythonLogLevel;
    const std::unique_ptr<Logger> _fallbackLogger;

    static constexpr int _getLogLevelValue(Level level) {
        return 10 + (level * 10);
    }

   public:

    LoggerWrapper(PyObject* pyLogger, int pythonLogLevel, Logger* fallbackLogger)
        : _pyLogger(pyLogger),
          _pythonLogLevel(pythonLogLevel),
          _fallbackLogger(fallbackLogger) {
        Py_XINCREF(_pyLogger);
    }

    LoggerWrapper(const LoggerWrapper&) = delete;
    LoggerWrapper(LoggerWrapper&&) noexcept = delete;
    LoggerWrapper& operator=(const LoggerWrapper&) = delete;
    LoggerWrapper& operator=(LoggerWrapper&&) = delete;

    virtual ~LoggerWrapper() {
        Py_XDECREF(_pyLogger);
    }

    bool isEnabled(Level level) {
        return _getLogLevelValue(level) >= _pythonLogLevel;
    }

    void log(Level level, int line, const std::string& message) {
        if (!Py_IsInitialized()) {
            // Python logger is unavailable - fallback to console logger
            _fallbackLogger->log(level, line, message);
        } else {
            PyGILState_STATE state = PyGILState_Ensure();

            try {
                switch (level) {
                    case Logger::LEVEL_DEBUG:
                        py::call_method<void>(_pyLogger, "debug", message.c_str());
                        break;
                    case Logger::LEVEL_INFO:
                        py::call_method<void>(_pyLogger, "info", message.c_str());
                        break;
                    case Logger::LEVEL_WARN:
                        py::call_method<void>(_pyLogger, "warning", message.c_str());
                        break;
                    case Logger::LEVEL_ERROR:
                        py::call_method<void>(_pyLogger, "error", message.c_str());
                        break;
                }

            } catch (const py::error_already_set& e) {
                _fallbackLogger->log(level, line, message);
            }

            PyGILState_Release(state);
        }
    }
};

class LoggerWrapperFactory : public LoggerFactory {
    std::unique_ptr<LoggerFactory> _fallbackLoggerFactory{new ConsoleLoggerFactory};
    PyObject* _pyLogger;
    Optional<int> _pythonLogLevel{Optional<int>::empty()};

    void initializePythonLogLevel() {
        PyGILState_STATE state = PyGILState_Ensure();

        try {
            int level = py::call_method<int>(_pyLogger, "getEffectiveLevel");
            _pythonLogLevel = Optional<int>::of(level);
        } catch (const py::error_already_set& e) {
            // Failed to get log level from _pyLogger, set it to empty to fallback to _fallbackLogger
            _pythonLogLevel = Optional<int>::empty();
        }

        PyGILState_Release(state);
    }

   public:
    LoggerWrapperFactory(py::object pyLogger) {
        _pyLogger = pyLogger.ptr();
        Py_XINCREF(_pyLogger);
        initializePythonLogLevel();
    }

    virtual ~LoggerWrapperFactory() {
        Py_XDECREF(_pyLogger);
    }

    Logger* getLogger(const std::string &fileName) {
        const auto fallbackLogger = _fallbackLoggerFactory->getLogger(fileName);
        if (_pythonLogLevel.is_present()) {
            return new LoggerWrapper(_pyLogger, _pythonLogLevel.value(), fallbackLogger);
        } else {
            return fallbackLogger;
        }
    }
};

static ClientConfiguration& ClientConfiguration_setLogger(ClientConfiguration& conf, py::object logger) {
    conf.setLogger(new LoggerWrapperFactory(logger));
    return conf;
}


void export_config() {
    using namespace boost::python;

    class_<ClientConfiguration>("ClientConfiguration")
            .def("authentication", &ClientConfiguration_setAuthentication, return_self<>())
            .def("operation_timeout_seconds", &ClientConfiguration::getOperationTimeoutSeconds)
            .def("operation_timeout_seconds", &ClientConfiguration::setOperationTimeoutSeconds, return_self<>())
            .def("connection_timeout", &ClientConfiguration::getConnectionTimeout)
            .def("connection_timeout", &ClientConfiguration::setConnectionTimeout, return_self<>())
            .def("io_threads", &ClientConfiguration::getIOThreads)
            .def("io_threads", &ClientConfiguration::setIOThreads, return_self<>())
            .def("message_listener_threads", &ClientConfiguration::getMessageListenerThreads)
            .def("message_listener_threads", &ClientConfiguration::setMessageListenerThreads, return_self<>())
            .def("concurrent_lookup_requests", &ClientConfiguration::getConcurrentLookupRequest)
            .def("concurrent_lookup_requests", &ClientConfiguration::setConcurrentLookupRequest, return_self<>())
            .def("log_conf_file_path", &ClientConfiguration::getLogConfFilePath, return_value_policy<copy_const_reference>())
            .def("log_conf_file_path", &ClientConfiguration::setLogConfFilePath, return_self<>())
            .def("use_tls", &ClientConfiguration::isUseTls)
            .def("use_tls", &ClientConfiguration::setUseTls, return_self<>())
            .def("tls_trust_certs_file_path", &ClientConfiguration::getTlsTrustCertsFilePath, return_value_policy<copy_const_reference>())
            .def("tls_trust_certs_file_path", &ClientConfiguration::setTlsTrustCertsFilePath, return_self<>())
            .def("tls_allow_insecure_connection", &ClientConfiguration::isTlsAllowInsecureConnection)
            .def("tls_allow_insecure_connection", &ClientConfiguration::setTlsAllowInsecureConnection, return_self<>())
            .def("tls_validate_hostname", &ClientConfiguration::setValidateHostName, return_self<>())
            .def("set_logger", &ClientConfiguration_setLogger, return_self<>())
            ;

    class_<ProducerConfiguration>("ProducerConfiguration")
            .def("producer_name", &ProducerConfiguration::getProducerName, return_value_policy<copy_const_reference>())
            .def("producer_name", &ProducerConfiguration::setProducerName, return_self<>())
            .def("schema", &ProducerConfiguration::getSchema, return_value_policy<copy_const_reference>())
            .def("schema", &ProducerConfiguration::setSchema, return_self<>())
            .def("send_timeout_millis", &ProducerConfiguration::getSendTimeout)
            .def("send_timeout_millis", &ProducerConfiguration::setSendTimeout, return_self<>())
            .def("initial_sequence_id", &ProducerConfiguration::getInitialSequenceId)
            .def("initial_sequence_id", &ProducerConfiguration::setInitialSequenceId, return_self<>())
            .def("compression_type", &ProducerConfiguration::getCompressionType)
            .def("compression_type", &ProducerConfiguration::setCompressionType, return_self<>())
            .def("max_pending_messages", &ProducerConfiguration::getMaxPendingMessages)
            .def("max_pending_messages", &ProducerConfiguration::setMaxPendingMessages, return_self<>())
            .def("max_pending_messages_across_partitions", &ProducerConfiguration::getMaxPendingMessagesAcrossPartitions)
            .def("max_pending_messages_across_partitions", &ProducerConfiguration::setMaxPendingMessagesAcrossPartitions, return_self<>())
            .def("block_if_queue_full", &ProducerConfiguration::getBlockIfQueueFull)
            .def("block_if_queue_full", &ProducerConfiguration::setBlockIfQueueFull, return_self<>())
            .def("partitions_routing_mode", &ProducerConfiguration::getPartitionsRoutingMode)
            .def("partitions_routing_mode", &ProducerConfiguration::setPartitionsRoutingMode, return_self<>())
            .def("lazy_start_partitioned_producers", &ProducerConfiguration::getLazyStartPartitionedProducers)
            .def("lazy_start_partitioned_producers", &ProducerConfiguration::setLazyStartPartitionedProducers, return_self<>())
            .def("batching_enabled", &ProducerConfiguration::getBatchingEnabled, return_value_policy<copy_const_reference>())
            .def("batching_enabled", &ProducerConfiguration::setBatchingEnabled, return_self<>())
            .def("batching_max_messages", &ProducerConfiguration::getBatchingMaxMessages, return_value_policy<copy_const_reference>())
            .def("batching_max_messages", &ProducerConfiguration::setBatchingMaxMessages, return_self<>())
            .def("batching_max_allowed_size_in_bytes", &ProducerConfiguration::getBatchingMaxAllowedSizeInBytes, return_value_policy<copy_const_reference>())
            .def("batching_max_allowed_size_in_bytes", &ProducerConfiguration::setBatchingMaxAllowedSizeInBytes, return_self<>())
            .def("batching_max_publish_delay_ms", &ProducerConfiguration::getBatchingMaxPublishDelayMs, return_value_policy<copy_const_reference>())
            .def("batching_max_publish_delay_ms", &ProducerConfiguration::setBatchingMaxPublishDelayMs, return_self<>())
            .def("property", &ProducerConfiguration::setProperty, return_self<>())
            .def("batching_type", &ProducerConfiguration::setBatchingType, return_self<>())
            .def("batching_type", &ProducerConfiguration::getBatchingType)
            .def("encryption_key", &ProducerConfiguration::addEncryptionKey, return_self<>())
            .def("crypto_key_reader", &ProducerConfiguration_setCryptoKeyReader, return_self<>())
            ;

    class_<ConsumerConfiguration>("ConsumerConfiguration")
            .def("consumer_type", &ConsumerConfiguration::getConsumerType)
            .def("consumer_type", &ConsumerConfiguration::setConsumerType, return_self<>())
            .def("schema", &ConsumerConfiguration::getSchema, return_value_policy<copy_const_reference>())
            .def("schema", &ConsumerConfiguration::setSchema, return_self<>())
            .def("message_listener", &ConsumerConfiguration_setMessageListener, return_self<>())
            .def("receiver_queue_size", &ConsumerConfiguration::getReceiverQueueSize)
            .def("receiver_queue_size", &ConsumerConfiguration::setReceiverQueueSize)
            .def("max_total_receiver_queue_size_across_partitions", &ConsumerConfiguration::getMaxTotalReceiverQueueSizeAcrossPartitions)
            .def("max_total_receiver_queue_size_across_partitions", &ConsumerConfiguration::setMaxTotalReceiverQueueSizeAcrossPartitions)
            .def("consumer_name", &ConsumerConfiguration::getConsumerName, return_value_policy<copy_const_reference>())
            .def("consumer_name", &ConsumerConfiguration::setConsumerName)
            .def("unacked_messages_timeout_ms", &ConsumerConfiguration::getUnAckedMessagesTimeoutMs)
            .def("unacked_messages_timeout_ms", &ConsumerConfiguration::setUnAckedMessagesTimeoutMs)
            .def("negative_ack_redelivery_delay_ms", &ConsumerConfiguration::getNegativeAckRedeliveryDelayMs)
            .def("negative_ack_redelivery_delay_ms", &ConsumerConfiguration::setNegativeAckRedeliveryDelayMs)
            .def("broker_consumer_stats_cache_time_ms", &ConsumerConfiguration::getBrokerConsumerStatsCacheTimeInMs)
            .def("broker_consumer_stats_cache_time_ms", &ConsumerConfiguration::setBrokerConsumerStatsCacheTimeInMs)
            .def("pattern_auto_discovery_period", &ConsumerConfiguration::getPatternAutoDiscoveryPeriod)
            .def("pattern_auto_discovery_period", &ConsumerConfiguration::setPatternAutoDiscoveryPeriod)
            .def("read_compacted", &ConsumerConfiguration::isReadCompacted)
            .def("read_compacted", &ConsumerConfiguration::setReadCompacted)
            .def("property", &ConsumerConfiguration::setProperty, return_self<>())
            .def("subscription_initial_position", &ConsumerConfiguration::getSubscriptionInitialPosition)
            .def("subscription_initial_position", &ConsumerConfiguration::setSubscriptionInitialPosition)
            .def("crypto_key_reader", &ConsumerConfiguration_setCryptoKeyReader, return_self<>())
            .def("replicate_subscription_state_enabled", &ConsumerConfiguration::setReplicateSubscriptionStateEnabled)
            .def("replicate_subscription_state_enabled", &ConsumerConfiguration::isReplicateSubscriptionStateEnabled)
            ;

    class_<ReaderConfiguration>("ReaderConfiguration")
            .def("reader_listener", &ReaderConfiguration_setReaderListener, return_self<>())
            .def("schema", &ReaderConfiguration::getSchema, return_value_policy<copy_const_reference>())
            .def("schema", &ReaderConfiguration::setSchema, return_self<>())
            .def("receiver_queue_size", &ReaderConfiguration::getReceiverQueueSize)
            .def("receiver_queue_size", &ReaderConfiguration::setReceiverQueueSize)
            .def("reader_name", &ReaderConfiguration::getReaderName, return_value_policy<copy_const_reference>())
            .def("reader_name", &ReaderConfiguration::setReaderName)
            .def("subscription_role_prefix", &ReaderConfiguration::getSubscriptionRolePrefix, return_value_policy<copy_const_reference>())
            .def("subscription_role_prefix", &ReaderConfiguration::setSubscriptionRolePrefix)
            .def("read_compacted", &ReaderConfiguration::isReadCompacted)
            .def("read_compacted", &ReaderConfiguration::setReadCompacted)
            .def("crypto_key_reader", &ReaderConfiguration_setCryptoKeyReader, return_self<>())
            ;
}
