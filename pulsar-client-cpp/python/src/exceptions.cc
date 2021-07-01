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
#include <map>

#include "utils.h"

static PyObject* basePulsarException = nullptr;
std::map<Result, PyObject*> exceptions;

PyObject* createExceptionClass(const char* name, PyObject* baseTypeObj = PyExc_Exception) {
    using namespace boost::python;

    std::string fullName = "_pulsar.";
    fullName += name;

    PyObject* typeObj = PyErr_NewException(const_cast<char*>(fullName.c_str()),
                                           baseTypeObj, nullptr);
    if (!typeObj) throw_error_already_set();
    scope().attr(name) = handle<>(borrowed(typeObj));
    return typeObj;
}

PyObject* get_exception_class(Result result) {
    return exceptions[result];
}

void export_exceptions() {
    using namespace boost::python;

    basePulsarException = createExceptionClass("PulsarException");

    exceptions[ResultUnknownError] = createExceptionClass("UnknownError", basePulsarException);
    exceptions[ResultInvalidConfiguration] = createExceptionClass("InvalidConfiguration", basePulsarException);
    exceptions[ResultTimeout] = createExceptionClass("Timeout", basePulsarException);
    exceptions[ResultLookupError] = createExceptionClass("LookupError", basePulsarException);
    exceptions[ResultConnectError] = createExceptionClass("ConnectError", basePulsarException);
    exceptions[ResultReadError] = createExceptionClass("ReadError", basePulsarException);
    exceptions[ResultAuthenticationError] = createExceptionClass("AuthenticationError", basePulsarException);
    exceptions[ResultAuthorizationError] = createExceptionClass("AuthorizationError", basePulsarException);
    exceptions[ResultErrorGettingAuthenticationData] = createExceptionClass("ErrorGettingAuthenticationData", basePulsarException);
    exceptions[ResultBrokerMetadataError] = createExceptionClass("BrokerMetadataError", basePulsarException);
    exceptions[ResultBrokerPersistenceError] = createExceptionClass("BrokerPersistenceError", basePulsarException);
    exceptions[ResultChecksumError] = createExceptionClass("ChecksumError", basePulsarException);
    exceptions[ResultConsumerBusy] = createExceptionClass("ConsumerBusy", basePulsarException);
    exceptions[ResultNotConnected] = createExceptionClass("NotConnected", basePulsarException);
    exceptions[ResultAlreadyClosed] = createExceptionClass("AlreadyClosed", basePulsarException);
    exceptions[ResultInvalidMessage] = createExceptionClass("InvalidMessage", basePulsarException);
    exceptions[ResultConsumerNotInitialized] = createExceptionClass("ConsumerNotInitialized", basePulsarException);
    exceptions[ResultProducerNotInitialized] = createExceptionClass("ProducerNotInitialized", basePulsarException);
    exceptions[ResultProducerBusy] = createExceptionClass("ProducerBusy", basePulsarException);
    exceptions[ResultTooManyLookupRequestException] = createExceptionClass("TooManyLookupRequestException", basePulsarException);
    exceptions[ResultInvalidTopicName] = createExceptionClass("InvalidTopicName", basePulsarException);
    exceptions[ResultInvalidUrl] = createExceptionClass("InvalidUrl", basePulsarException);
    exceptions[ResultServiceUnitNotReady] = createExceptionClass("ServiceUnitNotReady", basePulsarException);
    exceptions[ResultOperationNotSupported] = createExceptionClass("OperationNotSupported", basePulsarException);
    exceptions[ResultProducerBlockedQuotaExceededError] = createExceptionClass("ProducerBlockedQuotaExceededError", basePulsarException);
    exceptions[ResultProducerBlockedQuotaExceededException] = createExceptionClass("ProducerBlockedQuotaExceededException", basePulsarException);
    exceptions[ResultProducerQueueIsFull] = createExceptionClass("ProducerQueueIsFull", basePulsarException);
    exceptions[ResultMessageTooBig] = createExceptionClass("MessageTooBig", basePulsarException);
    exceptions[ResultTopicNotFound] = createExceptionClass("TopicNotFound", basePulsarException);
    exceptions[ResultSubscriptionNotFound] = createExceptionClass("SubscriptionNotFound", basePulsarException);
    exceptions[ResultConsumerNotFound] = createExceptionClass("ConsumerNotFound", basePulsarException);
    exceptions[ResultUnsupportedVersionError] = createExceptionClass("UnsupportedVersionError", basePulsarException);
    exceptions[ResultTopicTerminated] = createExceptionClass("TopicTerminated", basePulsarException);
    exceptions[ResultCryptoError] = createExceptionClass("CryptoError", basePulsarException);
    exceptions[ResultIncompatibleSchema] = createExceptionClass("IncompatibleSchema", basePulsarException);
    exceptions[ResultConsumerAssignError] = createExceptionClass("ConsumerAssignError", basePulsarException);
    exceptions[ResultCumulativeAcknowledgementNotAllowedError] = createExceptionClass("CumulativeAcknowledgementNotAllowedError", basePulsarException);
    exceptions[ResultTransactionCoordinatorNotFoundError] = createExceptionClass("TransactionCoordinatorNotFoundError", basePulsarException);
    exceptions[ResultInvalidTxnStatusError] = createExceptionClass("InvalidTxnStatusError", basePulsarException);
    exceptions[ResultNotAllowedError] = createExceptionClass("NotAllowedError", basePulsarException);
    exceptions[ResultTransactionConflict] = createExceptionClass("TransactionConflict", basePulsarException);
    exceptions[ResultTransactionNotFound] = createExceptionClass("TransactionNotFound", basePulsarException);
    exceptions[ResultProducerFenced] = createExceptionClass("ProducerFenced", basePulsarException);
    exceptions[ResultMemoryBufferIsFull] = createExceptionClass("MemoryBufferIsFull", basePulsarException);
}
