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
#include "NamedEntity.h"
#include "LogUtils.h"
#include "PartitionedProducerImpl.h"

#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find.hpp>
#include <boost/make_shared.hpp>
#include <lib/TopicName.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <exception>

DECLARE_LOG_OBJECT()
namespace pulsar {

typedef boost::unique_lock<boost::mutex> Lock;
// static members
CURL* TopicName::curl = NULL;
boost::mutex TopicName::curlHandleMutex;

CURL* TopicName::getCurlHandle() {
    if (curl == NULL) {
        // this handle can not be shared across threads, so had to get here everytime
        curl = curl_easy_init();
    }
    return curl;
}
//********************************************************************
TopicName::TopicName() {}

bool TopicName::init(const std::string& topicName) {
    topicName_ = topicName;
    if (topicName.find("://") == std::string::npos) {
        LOG_ERROR("Topic name is not valid, domain not present - " << topicName);
        return false;
    }
    parse(topicName_, domain_, property_, cluster_, namespacePortion_, localName_);
    if (localName_.empty()) {
        LOG_ERROR("Topic name is not valid, topic name is empty - " << topicName_);
        return false;
    }
    namespaceName_ = NamespaceName::get(property_, cluster_, namespacePortion_);
    return true;
}
void TopicName::parse(const std::string& topicName, std::string& domain, std::string& property,
                      std::string& cluster, std::string& namespacePortion, std::string& localName) {
    std::string topicNameCopy = topicName;
    boost::replace_first(topicNameCopy, "://", "/");
    std::vector<std::string> pathTokens;
    boost::algorithm::split(pathTokens, topicNameCopy, boost::algorithm::is_any_of("/"));
    if (pathTokens.size() < 5) {
        LOG_ERROR("Topic name is not valid, does not have enough parts - " << topicName);
        return;
    }
    domain = pathTokens[0];
    property = pathTokens[1];
    cluster = pathTokens[2];
    namespacePortion = pathTokens[3];
    size_t slashIndex = -1;
    // find four '/', whatever is left is topic local name
    for (int i = 0; i < 4; i++) {
        slashIndex = topicNameCopy.find('/', slashIndex + 1);
    }
    // get index to next char to '/'
    slashIndex++;
    localName = topicNameCopy.substr(slashIndex, (topicNameCopy.size() - slashIndex));
}
std::string TopicName::getEncodedName(const std::string& nameBeforeEncoding) {
    Lock lock(curlHandleMutex);
    std::string nameAfterEncoding;
    if (getCurlHandle()) {
        char* encodedName =
            curl_easy_escape(getCurlHandle(), nameBeforeEncoding.c_str(), nameBeforeEncoding.size());
        if (encodedName) {
            nameAfterEncoding.assign(encodedName);
            curl_free(encodedName);
        } else {
            LOG_ERROR("Unable to encode the name using curl_easy_escape, name - " << nameBeforeEncoding);
        }
    } else {
        LOG_ERROR("Unable to get CURL handle to encode the name - " << nameBeforeEncoding);
    }
    return nameAfterEncoding;
}

std::string TopicName::getDomain() { return domain_; }

std::string TopicName::getProperty() { return property_; }

std::string TopicName::getCluster() { return cluster_; }

std::string TopicName::getNamespacePortion() { return namespacePortion_; }

std::string TopicName::getLocalName() { return localName_; }

std::string TopicName::getEncodedLocalName() { return getEncodedName(localName_); }

bool TopicName::operator==(const TopicName& other) {
    return (this->topicName_.compare(other.topicName_) == 0);
}

bool TopicName::validate() {
    // check domain matches to "persistent", in future check "memory" when server is ready
    if (domain_.compare("persistent") != 0) {
        return false;
    }
    if (!property_.empty() && !cluster_.empty() && !namespacePortion_.empty() && !localName_.empty()) {
        return NamedEntity::checkName(property_) && NamedEntity::checkName(cluster_) &&
               NamedEntity::checkName(namespacePortion_);
    } else {
        return false;
    }
}

boost::shared_ptr<TopicName> TopicName::get(const std::string& topicName) {
    boost::shared_ptr<TopicName> ptr(new TopicName());
    if (!ptr->init(topicName)) {
        LOG_ERROR("Topic name initialization failed");
        return boost::shared_ptr<TopicName>();
    }
    if (ptr->validate()) {
        return ptr;
    } else {
        LOG_ERROR("Topic name validation Failed");
        return boost::shared_ptr<TopicName>();
    }
}

// TODO - for now return empty string if there's any error in format, later think about better error handling
std::string TopicName::getLookupName() {
    std::stringstream ss;
    std::string seperator("/");
    ss << domain_ << seperator << property_ << seperator << cluster_ << seperator << namespacePortion_
       << seperator << getEncodedLocalName();
    return ss.str();
}

std::string TopicName::toString() {
    std::stringstream ss;
    std::string seperator("/");
    ss << domain_ << "://" << property_ << seperator << cluster_ << seperator << namespacePortion_
       << seperator << localName_;
    return ss.str();
}

const std::string TopicName::getTopicPartitionName(unsigned int partition) {
    std::stringstream topicPartitionName;
    // make this topic name as well
    topicPartitionName << toString() << PartitionedProducerImpl::PARTITION_NAME_SUFFIX << partition;
    return topicPartitionName.str();
}
}  // namespace pulsar
