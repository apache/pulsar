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

#ifndef _PULSAR_LOOKUP_DATA_RESULT_HEADER_
#define _PULSAR_LOOKUP_DATA_RESULT_HEADER_
#include <string>

namespace pulsar {
class LookupDataResult;
typedef boost::shared_ptr<LookupDataResult> LookupDataResultPtr;
typedef Promise<Result, LookupDataResultPtr> LookupDataResultPromise;
typedef boost::shared_ptr<LookupDataResultPromise> LookupDataResultPromisePtr;

class LookupDataResult {
 public:
    void setBrokerUrl(const std::string& brokerUrl) {
        brokerUrl_ = brokerUrl;
    }
    void setBrokerUrlSsl(const std::string& brokerUrlSsl) {
        brokerUrlSsl_ = brokerUrlSsl;
    }
    std::string getBrokerUrl() {
        return brokerUrl_;
    }
    std::string getBrokerUrlSsl() {
        return brokerUrlSsl_;
    }

    bool isAuthoritative() const {
        return authoritative;
    }

    void setAuthoritative(bool authoritative) {
        this->authoritative = authoritative;
    }

    int getPartitions() const {
        return partitions;
    }

    void setPartitions(int partitions) {
        this->partitions = partitions;
    }

    bool isRedirect() const {
        return redirect;
    }

    void setRedirect(bool redirect) {
        this->redirect = redirect;
    }

 private:
    std::string brokerUrl_;
    std::string brokerUrlSsl_;
    int partitions;
    bool authoritative;
    bool redirect;
};

}

#endif // _PULSAR_LOOKUP_DATA_RESULT_HEADER_
