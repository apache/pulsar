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
#include "ServiceURI.h"
#include <stdexcept>

namespace pulsar {

static void processAddress(std::string& address, PulsarScheme scheme) {
    const auto posOfSlash = address.find('/');
    if (posOfSlash != std::string::npos) {
        // ignore the path part
        address.erase(posOfSlash);
    }
    auto fail = [&address] { throw std::invalid_argument("invalid hostname: " + address); };

    const auto posOfColon = address.find(':');
    if (posOfColon != std::string::npos) {
        if (address.find(':', posOfColon + 1) != std::string::npos) {
            fail();
        }
        try {
            const auto port = std::stoi(address.substr(posOfColon + 1));
            if (port < 0 || port > 65535) {
                throw std::invalid_argument("");
            }
        } catch (const std::invalid_argument& ignored) {
            fail();
        }
    } else {
        address = address + ":" + std::to_string(scheme::getDefaultPort(scheme));
    }
    if (!address.empty()) {
        address = scheme::getSchemeString(scheme) + address;
    }
}

auto ServiceURI::parse(const std::string& uriString) -> DataType {
    size_t pos = uriString.find("://");
    if (pos == std::string::npos) {
        throw std::invalid_argument("The scheme part is missing: " + uriString);
    }
    if (pos == 0) {
        throw std::invalid_argument("Expected scheme name at index 0: " + uriString);
    }
    const auto scheme = scheme::toScheme(uriString.substr(0, pos));

    pos += 3;  // now it points to the end of "://"
    if (pos < uriString.size() && uriString[pos] == '/') {
        throw std::invalid_argument("authority component is missing in service uri: " + uriString);
    }

    std::vector<std::string> addresses;
    while (pos < uriString.size()) {
        const size_t endPos = uriString.find(',', pos);
        if (endPos == std::string::npos) {
            addresses.emplace_back(uriString.substr(pos, endPos - pos));
            break;
        }
        addresses.emplace_back(uriString.substr(pos, endPos - pos));
        pos = endPos + 1;
    }

    bool hasEmptyAddress = false;
    for (auto& address : addresses) {
        processAddress(address, scheme);
        if (address.empty()) {
            hasEmptyAddress = true;
        }
    }
    if (hasEmptyAddress) {
        auto originalAddresses = addresses;
        addresses.clear();
        for (const auto& address : originalAddresses) {
            if (!address.empty()) {
                addresses.emplace_back(address);
            }
        }
    }
    if (addresses.empty()) {
        throw std::invalid_argument("No service url is provided yet");
    }
    return std::make_pair(scheme, addresses);
}

}  // namespace pulsar
