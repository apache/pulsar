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
#pragma once

#include <assert.h>
#include <atomic>
#include "ServiceURI.h"

namespace pulsar {

class ServiceNameResolver {
   public:
    ServiceNameResolver(const std::string& uriString)
        : serviceUri_(uriString), numAddresses_(serviceUri_.getServiceHosts().size()) {
        assert(numAddresses_ > 0);  // the validation has been done in ServiceURI
    }

    ServiceNameResolver(const ServiceNameResolver&) = delete;
    ServiceNameResolver& operator=(const ServiceNameResolver&) = delete;

    bool useTls() const noexcept {
        return serviceUri_.getScheme() == PulsarScheme::PULSAR_SSL ||
               serviceUri_.getScheme() == PulsarScheme::HTTPS;
    }

    bool useHttp() const noexcept {
        return serviceUri_.getScheme() == PulsarScheme::HTTP ||
               serviceUri_.getScheme() == PulsarScheme::HTTPS;
    }

    const std::string& resolveHost() {
        return serviceUri_.getServiceHosts()[(numAddresses_ == 1) ? 0 : (index_++ % numAddresses_)];
    }

   private:
    const ServiceURI serviceUri_;
    const size_t numAddresses_;
    std::atomic_size_t index_{0};

    friend class PulsarFriend;
};

}  // namespace pulsar
