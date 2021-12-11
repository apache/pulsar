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
#include "Url.h"

#include <boost/regex.hpp>
#include <sstream>

namespace pulsar {

static const std::map<std::string, int> initDefaultPortsMap() {
    std::map<std::string, int> defaultPortsMap;
    defaultPortsMap["http"] = 80;
    defaultPortsMap["https"] = 443;
    defaultPortsMap["pulsar"] = 6650;
    defaultPortsMap["pulsar+ssl"] = 6651;
    return defaultPortsMap;
}

static const std::map<std::string, int>& defaultPortsMap() {
    static std::map<std::string, int> defaultPortsMap = initDefaultPortsMap();
    return defaultPortsMap;
}

bool Url::parse(const std::string& urlStr, Url& url) {
    std::vector<std::string> values;
    static const boost::regex expression(
        //       proto                 host               port
        "^(\?:([^:/\?#]+)://)\?(\\w+[^/\?#:]*)(\?::(\\d+))\?"
        //       path                  file       parameters
        "(/\?(\?:[^\?#/]*/)*)\?([^\?#]*)\?(\\\?(.*))\?");

    boost::cmatch groups;
    if (!boost::regex_match(urlStr.c_str(), groups, expression)) {
        // Invalid url
        return false;
    }

    url.protocol_ = std::string(groups[1].first, groups[1].second);
    url.host_ = std::string(groups[2].first, groups[2].second);
    std::string portStr(groups[3].first, groups[3].second);
    url.pathWithoutFile_ = std::string(groups[4].first, groups[4].second);
    url.file_ = std::string(groups[5].first, groups[5].second);
    url.parameter_ = std::string(groups[6].first, groups[6].second);
    url.path_ = url.pathWithoutFile_ + url.file_;

    if (!portStr.empty()) {
        url.port_ = atoi(groups[3].first);
    } else {
        std::map<std::string, int>::const_iterator it = defaultPortsMap().find(url.protocol_);
        if (it != defaultPortsMap().end()) {
            url.port_ = it->second;
        } else {
            // Invalid port
            return false;
        }
    }

    return true;
}

const std::string& Url::protocol() const { return protocol_; }

const std::string& Url::host() const { return host_; }

const int Url::port() const { return port_; }

const std::string& Url::path() const { return path_; }

const std::string& Url::pathWithoutFile() const { return pathWithoutFile_; }

const std::string& Url::file() const { return file_; }

const std::string& Url::parameter() const { return parameter_; }

std::string Url::hostPort() const {
    std::stringstream ss;
    ss << host_ << ':' << port_;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const Url& obj) {
    os << "Url [Host = " << obj.host() << ", Protocol = " << obj.protocol() << ", Port = " << obj.port()
       << "]";
    return os;
}

}  // namespace pulsar
