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

#ifndef _PULSAR_DESTINATION_NAME_HEADER_
#define _PULSAR_DESTINATION_NAME_HEADER_

#include "NamespaceName.h"
#include "ServiceUnitId.h"

#include <string>
#include <curl/curl.h>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>

#pragma GCC visibility push(default)

namespace pulsar {
    class DestinationName : public ServiceUnitId {
    private:
        std::string destination_;
        std::string domain_;
        std::string property_;
        std::string cluster_;
        std::string namespacePortion_;
        std::string localName_;
        boost::shared_ptr<NamespaceName> namespaceName_;

    public:
        std::string getLookupName();
        std::string getDomain();
        std::string getProperty();
        std::string getCluster();
        std::string getNamespacePortion();
        std::string getLocalName();
        std::string getEncodedLocalName();
        std::string toString();
        static boost::shared_ptr<DestinationName> get(const std::string& destination);
        bool operator ==(const DestinationName& other);
        static std::string getEncodedName(const std::string& nameBeforeEncoding);
        const std::string getTopicPartitionName(unsigned int partition);
    private:
        static CURL* getCurlHandle();
        static CURL* curl;
        static boost::mutex curlHandleMutex;
        static void parse(const std::string& destinationName,
                          std::string& domain,
                          std::string& property,
                          std::string& cluster,
                          std::string& namespacePortion,
                          std::string& localName);
        DestinationName();
        bool validateDestination();
        bool init(const std::string& destinationName);
    };
    typedef  boost::shared_ptr<DestinationName> DestinationNamePtr;
}
// end of namespace pulsar

#pragma GCC visibility pop

#endif //_PULSAR_DESTINATION_NAME_HEADER_
