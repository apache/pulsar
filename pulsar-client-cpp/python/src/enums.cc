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


void export_enums() {
    using namespace boost::python;

    enum_<ProducerConfiguration::PartitionsRoutingMode>("PartitionsRoutingMode")
            .value("UseSinglePartition", ProducerConfiguration::UseSinglePartition)
            .value("RoundRobinDistribution", ProducerConfiguration::RoundRobinDistribution)
            .value("CustomPartition", ProducerConfiguration::CustomPartition)
            ;

    enum_<CompressionType>("CompressionType")
            .value("NONE", CompressionNone) // Don't use 'None' since it's a keyword in py3
            .value("LZ4", CompressionLZ4)
            .value("ZLib", CompressionZLib)
            .value("ZSTD", CompressionZSTD)
            ;

    enum_<ConsumerType>("ConsumerType")
            .value("Exclusive", ConsumerExclusive)
            .value("Shared", ConsumerShared)
            .value("Failover", ConsumerFailover)
            ;

    enum_<Result >("Result", "Collection of return codes")
            .value("Ok", ResultOk)
            .value("UnknownError", ResultUnknownError)
            .value("InvalidConfiguration", ResultInvalidConfiguration)
            .value("Timeout", ResultTimeout)
            .value("LookupError", ResultLookupError)
            .value("ConnectError", ResultConnectError)
            .value("ReadError", ResultReadError)
            .value("AuthenticationError", ResultAuthenticationError)
            .value("AuthorizationError", ResultAuthorizationError)
            .value("ErrorGettingAuthenticationData", ResultErrorGettingAuthenticationData)
            .value("BrokerMetadataError", ResultBrokerMetadataError)
            .value("BrokerPersistenceError", ResultBrokerPersistenceError)
            .value("ChecksumError", ResultChecksumError)
            .value("ConsumerBusy", ResultConsumerBusy)
            .value("NotConnected", ResultNotConnected)
            .value("AlreadyClosed", ResultAlreadyClosed)
            .value("InvalidMessage", ResultInvalidMessage)
            .value("ConsumerNotInitialized", ResultConsumerNotInitialized)
            .value("ProducerNotInitialized", ResultProducerNotInitialized)
            .value("TooManyLookupRequestException", ResultTooManyLookupRequestException)
            .value("InvalidTopicName", ResultInvalidTopicName)
            .value("InvalidUrl", ResultInvalidUrl)
            .value("ServiceUnitNotReady", ResultServiceUnitNotReady)
            .value("OperationNotSupported", ResultOperationNotSupported)
            .value("ProducerBlockedQuotaExceededError", ResultProducerBlockedQuotaExceededError)
            .value("ProducerBlockedQuotaExceededException", ResultProducerBlockedQuotaExceededException)
            .value("ProducerQueueIsFull", ResultProducerQueueIsFull)
            .value("MessageTooBig", ResultMessageTooBig)
            .value("TopicNotFound", ResultTopicNotFound)
            .value("SubscriptionNotFound", ResultSubscriptionNotFound)
            .value("ConsumerNotFound", ResultConsumerNotFound)
            .value("UnsupportedVersionError", ResultUnsupportedVersionError)
            ;

    enum_<SchemaType>("SchemaType", "Supported schema types")
            .value("NONE", NONE)
            .value("STRING", STRING)
            .value("INT8", INT8)
            .value("INT16", INT16)
            .value("INT32", INT32)
            .value("INT64", INT64)
            .value("FLOAT", FLOAT)
            .value("DOUBLE", DOUBLE)
            .value("BYTES", BYTES)
            .value("JSON", JSON)
            .value("PROTOBUF", PROTOBUF)
            .value("AVRO", AVRO)
            .value("AUTO_CONSUME", AUTO_CONSUME)
            .value("AUTO_PUBLISH", AUTO_PUBLISH)
            .value("KEY_VALUE", KEY_VALUE)
            ;

    enum_<InitialPosition>("InitialPosition", "Supported initial position")
            .value("Latest", InitialPositionLatest)
            .value("Earliest", InitialPositionEarliest)
            ;
}
