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
#ifndef _PULSAR_LOOKUP_DATA_RESULT_HEADER_
#define _PULSAR_LOOKUP_DATA_RESULT_HEADER_
#include <string>
#include <lib/Future.h>
#include <pulsar/Result.h>

#include <iostream>
#include <memory>

namespace pulsar {
class LookupDataResult;
typedef std::shared_ptr<LookupDataResult> LookupDataResultPtr;
typedef Promise<Result, LookupDataResultPtr> LookupDataResultPromise;
typedef std::shared_ptr<LookupDataResultPromise> LookupDataResultPromisePtr;

class LookupDataResult {
   public:
    void setBrokerUrl(const std::string& brokerUrl) { brokerUrl_ = brokerUrl; }
    void setBrokerUrlTls(const std::string& brokerUrlTls) { brokerUrlTls_ = brokerUrlTls; }
    const std::string& getBrokerUrl() const { return brokerUrl_; }
    const std::string& getBrokerUrlTls() const { return brokerUrlTls_; }

    bool isAuthoritative() const { return authoritative; }

    void setAuthoritative(bool authoritative) { this->authoritative = authoritative; }

    int getPartitions() const { return partitions; }

    void setPartitions(int partitions) { this->partitions = partitions; }

    bool isRedirect() const { return redirect; }

    void setRedirect(bool redirect) { this->redirect = redirect; }

    bool shouldProxyThroughServiceUrl() const { return proxyThroughServiceUrl_; }

    void setShouldProxyThroughServiceUrl(bool proxyThroughServiceUrl) {
        proxyThroughServiceUrl_ = proxyThroughServiceUrl;
    }

   private:
    friend inline std::ostream& operator<<(std::ostream& os, const LookupDataResult& b);
    std::string brokerUrl_;
    std::string brokerUrlTls_;
    int partitions;
    bool authoritative;
    bool redirect;

    bool proxyThroughServiceUrl_;
};

std::ostream& operator<<(std::ostream& os, const LookupDataResult& b) {
    os << "{ LookupDataResult [brokerUrl_ = " << b.brokerUrl_ << "] [brokerUrlTls_ = " << b.brokerUrlTls_
       << "] [partitions = " << b.partitions << "] [authoritative = " << b.authoritative
       << "] [redirect = " << b.redirect << "] proxyThroughServiceUrl = " << b.proxyThroughServiceUrl_
       << "] }";
    return os;
}
}  // namespace pulsar

#endif  // _PULSAR_LOOKUP_DATA_RESULT_HEADER_
