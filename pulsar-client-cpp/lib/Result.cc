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
#include <pulsar/defines.h>
#include <pulsar/Result.h>

#include <iostream>

namespace pulsar {

const char* strResult(Result result) {
    switch (result) {
        case ResultOk:
            return "Ok";

        case ResultUnknownError:
            return "UnknownError";

        case ResultInvalidConfiguration:
            return "InvalidConfiguration";

        case ResultTimeout:
            return "TimeOut";

        case ResultLookupError:
            return "LookupError";

        case ResultConnectError:
            return "ConnectError";

        case ResultAuthenticationError:
            return "AuthenticationError";

        case ResultAuthorizationError:
            return "AuthorizationError";

        case ResultErrorGettingAuthenticationData:
            return "ErrorGettingAuthenticationData";

        case ResultBrokerMetadataError:
            return "BrokerMetadataError";

        case ResultBrokerPersistenceError:
            return "BrokerPersistenceError";

        case ResultConsumerBusy:
            return "ConsumerBusy";

        case ResultNotConnected:
            return "NotConnected";

        case ResultReadError:
            return "ReadError";

        case ResultAlreadyClosed:
            return "AlreadyClosed";

        case ResultInvalidMessage:
            return "InvalidMessage";

        case ResultConsumerNotInitialized:
            return "ConsumerNotInitialized";

        case ResultProducerNotInitialized:
            return "ProducerNotInitialized";

        case ResultInvalidTopicName:
            return "InvalidTopicName";

        case ResultServiceUnitNotReady:
            return "ServiceUnitNotReady";

        case ResultInvalidUrl:
            return "InvalidUrl";

        case ResultChecksumError:
            return "ChecksumError";

        case ResultTooManyLookupRequestException:
            return "TooManyLookupRequestException";

        case ResultOperationNotSupported:
            return "OperationNotSupported";

        case ResultProducerBlockedQuotaExceededError:
            return "ProducerBlockedQuotaExceededError";

        case ResultProducerBlockedQuotaExceededException:
            return "ProducerBlockedQuotaExceededException";

        case ResultProducerQueueIsFull:
            return "ProducerQueueIsFull";

        case ResultMessageTooBig:
            return "MessageTooBig";

        case ResultTopicNotFound:
            return "TopicNotFound";

        case ResultSubscriptionNotFound:
            return "SubscriptionNotFound";

        case ResultConsumerNotFound:
            return "ConsumerNotFound";

        case ResultUnsupportedVersionError:
            return "UnsupportedVersionError";

        case ResultTopicTerminated:
            return "TopicTerminated";

        case ResultCryptoError:
            return "CryptoError";

        case ResultProducerBusy:
            return "ProducerBusy";

        case ResultIncompatibleSchema:
            return "IncompatibleSchema";

        case ResultConsumerAssignError:
            return "ResultConsumerAssignError";

        case ResultCumulativeAcknowledgementNotAllowedError:
            return "ResultCumulativeAcknowledgementNotAllowedError";

        case ResultTransactionCoordinatorNotFoundError:
            return "ResultTransactionCoordinatorNotFoundError";

        case ResultInvalidTxnStatusError:
            return "ResultInvalidTxnStatusError";

        case ResultNotAllowedError:
            return "ResultNotAllowedError";

        case ResultTransactionConflict:
            return "ResultTransactionConflict";

        case ResultTransactionNotFound:
            return "ResultTransactionNotFound";

        case ResultProducerFenced:
            return "ResultProducerFenced";

        case ResultMemoryBufferIsFull:
            return "ResultMemoryBufferIsFull";
    };
    // NOTE : Do not add default case in the switch above. In future if we get new cases for
    // ServerError and miss them in the switch above we would like to get notified. Adding
    // return here to make the compiler happy.
    return "UnknownErrorCode";
}

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, Result result) { return s << strResult(result); }

}  // namespace pulsar
