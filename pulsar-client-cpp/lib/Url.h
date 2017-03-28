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

#ifndef LIB_URL_H_
#define LIB_URL_H_

#include <string>

#pragma GCC visibility push(default)

namespace pulsar {

/**
 * URL parsing utility
 */
class Url {
public:
    static bool parse(const std::string& urlStr, Url& url);

    const std::string& protocol() const;
    const std::string& host() const;
    const int port() const;
    const std::string& path() const;
    const std::string& file() const;
    const std::string& parameter() const;
    friend std::ostream& operator<<(std::ostream &os, const Url& obj);
private:
    std::string protocol_;
    std::string host_;
    int port_;
    std::string path_;
    std::string file_;
    std::string parameter_;
};

} // pulsar

#pragma GCC visibility pop

#endif /* LIB_URL_H_ */
