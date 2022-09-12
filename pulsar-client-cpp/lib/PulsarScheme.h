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

#include <stdexcept>
#include <string>

namespace pulsar {

enum PulsarScheme
{
    PULSAR,
    PULSAR_SSL,
    HTTP,
    HTTPS
};

namespace scheme {

inline PulsarScheme toScheme(const std::string& scheme) {
    if (scheme == "pulsar") {
        return PulsarScheme::PULSAR;
    } else if (scheme == "pulsar+ssl") {
        return PulsarScheme::PULSAR_SSL;
    } else if (scheme == "http") {
        return PulsarScheme::HTTP;
    } else if (scheme == "https") {
        return PulsarScheme::HTTPS;
    } else {
        throw std::invalid_argument("Invalid scheme: " + scheme);
    }
}

inline const char* getSchemeString(PulsarScheme scheme) {
    switch (scheme) {
        case PulsarScheme::PULSAR:
            return "pulsar://";
        case PulsarScheme::PULSAR_SSL:
            return "pulsar+ssl://";
        case PulsarScheme::HTTP:
            return "http://";
        case PulsarScheme::HTTPS:
            return "https://";
        default:
            return "unknown://";
    }
}

inline short getDefaultPort(PulsarScheme scheme) {
    switch (scheme) {
        case PulsarScheme::PULSAR:
            return 6650;
        case PulsarScheme::PULSAR_SSL:
            return 6651;
        case PulsarScheme::HTTP:
            return 8080;
        case PulsarScheme::HTTPS:
            return 8081;
        default:
            throw std::invalid_argument("Unexpected scheme: " + std::to_string(scheme));
    }
}

}  // namespace scheme

}  // namespace pulsar
