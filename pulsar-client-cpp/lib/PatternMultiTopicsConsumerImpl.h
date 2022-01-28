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
#ifndef PULSAR_PATTERN_MULTI_TOPICS_CONSUMER_HEADER
#define PULSAR_PATTERN_MULTI_TOPICS_CONSUMER_HEADER
#include "ConsumerImpl.h"
#include "ClientImpl.h"
#include <lib/TopicName.h>
#include <lib/NamespaceName.h>
#include "MultiTopicsConsumerImpl.h"
#include <memory>

#ifdef PULSAR_USE_BOOST_REGEX
#include <boost/regex.hpp>
#define PULSAR_REGEX_NAMESPACE boost
#else
#include <regex>
#define PULSAR_REGEX_NAMESPACE std
#endif

namespace pulsar {

class PatternMultiTopicsConsumerImpl;

class PatternMultiTopicsConsumerImpl : public MultiTopicsConsumerImpl {
   public:
    // currently we support topics under same namespace, so `patternString` is a regex,
    // which only contains after namespace part.
    // when subscribe, client will first get all topics that match given pattern.
    // `topics` contains the topics that match `patternString`.
    PatternMultiTopicsConsumerImpl(ClientImplPtr client, const std::string patternString,
                                   const std::vector<std::string>& topics,
                                   const std::string& subscriptionName, const ConsumerConfiguration& conf,
                                   const LookupServicePtr lookupServicePtr_);

    const PULSAR_REGEX_NAMESPACE::regex getPattern();

    void autoDiscoveryTimerTask(const boost::system::error_code& err);

    // filter input `topics` with given `pattern`, return matched topics
    static NamespaceTopicsPtr topicsPatternFilter(const std::vector<std::string>& topics,
                                                  const PULSAR_REGEX_NAMESPACE::regex& pattern);

    // Find out topics, which are in `list1` but not in `list2`.
    static NamespaceTopicsPtr topicsListsMinus(std::vector<std::string>& list1,
                                               std::vector<std::string>& list2);

    virtual void closeAsync(ResultCallback callback);
    virtual void start();
    virtual void shutdown();

   private:
    const std::string patternString_;
    const PULSAR_REGEX_NAMESPACE::regex pattern_;
    typedef std::shared_ptr<boost::asio::deadline_timer> TimerPtr;
    TimerPtr autoDiscoveryTimer_;
    bool autoDiscoveryRunning_;
    NamespaceNamePtr namespaceName_;

    void resetAutoDiscoveryTimer();
    void timerGetTopicsOfNamespace(const Result result, const NamespaceTopicsPtr topics);
    void onTopicsAdded(NamespaceTopicsPtr addedTopics, ResultCallback callback);
    void onTopicsRemoved(NamespaceTopicsPtr removedTopics, ResultCallback callback);
    void handleOneTopicAdded(const Result result, const std::string& topic,
                             std::shared_ptr<std::atomic<int>> topicsNeedCreate, ResultCallback callback);
};

}  // namespace pulsar
#endif  // PULSAR_PATTERN_MULTI_TOPICS_CONSUMER_HEADER
