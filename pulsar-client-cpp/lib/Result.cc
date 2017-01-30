/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <pulsar/Result.h>

#include <iostream>

using namespace pulsar;

const char* pulsar::strResult(Result result) {
    switch (result) {
        case ResultOk:
            return "OK";

        case ResultUnknownError:
            return "UnknownError";

        case ResultInvalidConfiguration:
            return "InvalidConfiguration";

        case ResultTimeout:
            return "Timeout";

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

        default:
            return "UnknownErrorCode";
    };
}

#pragma GCC visibility push(default)

std::ostream& operator<<(std::ostream& s, Result result) {
    return s << strResult(result);
}

#pragma GCC visibility pop
