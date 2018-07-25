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
#ifndef _PULSAR_TOPIC_NAME_HEADER_
#define _PULSAR_TOPIC_NAME_HEADER_

#include "NamespaceName.h"
#include "ServiceUnitId.h"

#include <string>
#include <curl/curl.h>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>

#pragma GCC visibility push(default)

namespace pulsar {
class TopicName : public ServiceUnitId {
   private:
    std::string topicName_;
    std::string domain_;
    std::string property_;
    std::string cluster_;
    std::string namespacePortion_;
    std::string localName_;
    bool isV2Topic_;
    boost::shared_ptr<NamespaceName> namespaceName_;

   public:
    bool isV2Topic();
    std::string getLookupName();
    std::string getDomain();
    std::string getProperty();
    std::string getCluster();
    std::string getNamespacePortion();
    std::string getLocalName();
    std::string getEncodedLocalName();
    std::string toString();
    NamespaceNamePtr getNamespaceName();
    static boost::shared_ptr<TopicName> get(const std::string& topicName);
    bool operator==(const TopicName& other);
    static std::string getEncodedName(const std::string& nameBeforeEncoding);
    const std::string getTopicPartitionName(unsigned int partition);

   private:
    static CURL* getCurlHandle();
    static CURL* curl;
    static boost::mutex curlHandleMutex;
    static bool parse(const std::string& topicName, std::string& domain, std::string& property,
                      std::string& cluster, std::string& namespacePortion, std::string& localName);
    TopicName();
    bool validate();
    bool init(const std::string& topicName);
};
typedef boost::shared_ptr<TopicName> TopicNamePtr;
}  // namespace pulsar
// end of namespace pulsar

#pragma GCC visibility pop

#endif  //_PULSAR_TOPIC_NAME_HEADER_
