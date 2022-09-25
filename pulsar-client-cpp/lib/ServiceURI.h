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

#include <string>
#include <utility>
#include <vector>
#include "PulsarScheme.h"

namespace pulsar {

class ServiceURI {
   public:
    /**
     * @param uriString the URL string that is used to create a pulsar::Client object
     * @throws std::invalid_argument if `uriString` is invalid
     */
    ServiceURI(const std::string& uriString) : data_(parse(uriString)) {}

    PulsarScheme getScheme() const noexcept { return data_.first; }

    const std::vector<std::string>& getServiceHosts() const noexcept { return data_.second; }

   private:
    // The 2 elements of the pair are:
    // 1. The Scheme of the lookup protocol
    // 2. The available addresses, each item is like "pulsar://localhost:6650"
    using DataType = std::pair<PulsarScheme, std::vector<std::string>>;
    const DataType data_;

    static DataType parse(const std::string& uriString);
};

}  // namespace pulsar
