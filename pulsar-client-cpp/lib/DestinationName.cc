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

#include "DestinationName.h"
#include "NamedEntity.h"
#include "LogUtils.h"
#include "PartitionedProducerImpl.h"

#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find.hpp>
#include <boost/make_shared.hpp>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <exception>

DECLARE_LOG_OBJECT()
namespace pulsar {

    typedef boost::unique_lock<boost::mutex> Lock;
    // static members
    CURL* DestinationName::curl = NULL;
    boost::mutex DestinationName::curlHandleMutex;

    CURL* DestinationName::getCurlHandle() {
        if (curl == NULL) {
            // this handle can not be shared across threads, so had to get here everytime
            curl = curl_easy_init();
        }
        return curl;
    }
    //********************************************************************
    DestinationName::DestinationName() {
    }

    bool DestinationName::init(const std::string& destinationName) {
        destination_ = destinationName;
        if(destinationName.find("://") == std::string::npos){
            LOG_ERROR("Destination Name Invalid, domain not present - " << destinationName);
            return false;
        }
        parse(destination_, domain_, property_, cluster_, namespacePortion_, localName_);
        if(localName_.empty()) {
            LOG_ERROR("Destination Name is not valid, topic name is empty - " << destination_);
            return false;
        }
        namespaceName_ = NamespaceName::get(property_, cluster_, namespacePortion_);
        return true;
    }
    void DestinationName::parse(const std::string& destinationName,
                                std::string& domain,
                                std::string& property,
                                std::string& cluster,
                                std::string& namespacePortion,
                                std::string& localName) {
        std::string destinationNameCopy = destinationName;
        boost::replace_first(destinationNameCopy, "://", "/");
        std::vector<std::string> pathTokens;
        boost::algorithm::split(pathTokens, destinationNameCopy, boost::algorithm::is_any_of("/"));
        if (pathTokens.size() < 5) {
            LOG_ERROR("Destination Name Invalid, does not have enough parts - " << destinationName);
            return;
        }
        domain = pathTokens[0];
        property = pathTokens[1];
        cluster = pathTokens[2];
        namespacePortion = pathTokens[3];
        size_t slashIndex = -1;
        //find four '/', whatever is left is topic local name
        for(int i=0; i < 4; i++) {
            slashIndex = destinationNameCopy.find('/', slashIndex + 1);
        }
        // get index to next char to '/'
        slashIndex++;
        localName = destinationNameCopy.substr( slashIndex, (destinationNameCopy.size() - slashIndex));
    }
    std::string DestinationName::getEncodedName(const std::string& nameBeforeEncoding) {
        Lock lock(curlHandleMutex);
        std::string nameAfterEncoding;
        if(getCurlHandle()) {
            char *encodedName = curl_easy_escape(getCurlHandle(), nameBeforeEncoding.c_str(), nameBeforeEncoding.size());
            if(encodedName) {
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

    std::string DestinationName::getDomain() {
        return domain_;
    }

    std::string DestinationName::getProperty() {
        return property_;
    }

    std::string DestinationName::getCluster() {
        return cluster_;
    }

    std::string DestinationName::getNamespacePortion() {
        return namespacePortion_;
    }

    std::string DestinationName::getLocalName() {
        return localName_;
    }

    std::string DestinationName::getEncodedLocalName() {
        return getEncodedName(localName_);
    }

    bool DestinationName::operator ==(const DestinationName& other) {
        return (this->destination_.compare(other.destination_) == 0);
    }

    bool DestinationName::validateDestination() {
        // check domain matches to "persistent", in future check "memory" when server is ready
        if (domain_.compare("persistent") != 0) {
            return false;
        }
        if (!property_.empty() && !cluster_.empty() && !namespacePortion_.empty() && !localName_.empty()) {
            return NamedEntity::checkName(property_) && NamedEntity::checkName(cluster_)
                && NamedEntity::checkName(namespacePortion_);
        } else {
            return false;
        }
    }

    boost::shared_ptr<DestinationName> DestinationName::get(const std::string& destination) {
        boost::shared_ptr<DestinationName> ptr(new DestinationName());
        if (!ptr->init(destination)) {
            LOG_ERROR("Destination Name Initialization failed");
            return boost::shared_ptr<DestinationName>();
        }
        if (ptr->validateDestination()) {
            return ptr;
        } else {
            LOG_ERROR("Destination Name Validation Failed");
            return boost::shared_ptr<DestinationName>();
        }
    }

    //TODO - for now return empty string if there's any error in format, later think about better error handling
    std::string DestinationName::getLookupName() {
        std::stringstream ss;
        std::string seperator("/");
        ss << domain_ << seperator << property_ << seperator << cluster_ << seperator
           << namespacePortion_ << seperator << getEncodedLocalName();
        return ss.str();
    }

    std::string DestinationName::toString() {
        std::stringstream ss;
        std::string seperator("/");
        ss << domain_ << "://" << property_ << seperator << cluster_ << seperator
           << namespacePortion_ << seperator << localName_;
        return ss.str();
    }

    const std::string DestinationName::getTopicPartitionName(unsigned int partition) {
        std::stringstream topicPartitionName;
        // make this topic name as well
        topicPartitionName << toString()<< PartitionedProducerImpl::PARTITION_NAME_SUFFIX << partition ;
        return topicPartitionName.str();
    }
} //namespace pulsar
