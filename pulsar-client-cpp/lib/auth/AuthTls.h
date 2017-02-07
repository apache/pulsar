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

#ifndef PULSAR_AUTH_TLS_H_
#define PULSAR_AUTH_TLS_H_

#include <pulsar/Auth.h>
#include <lib/LogUtils.h>
#include <string>

namespace pulsar {

class AuthTls : public Authentication {
public:
    AuthTls(const std::string& params) {
    }

    ~AuthTls() {
    }

    static AuthenticationPtr create(const std::string& params) {
        return boost::shared_ptr<Authentication>(new AuthTls(params));
    }

    const std::string getAuthMethodName() const {
        return "tls";
    }

    Result getAuthData(std::string& authDataContent) const {
        authDataContent = "THIS_SHOULD_BE_REPLACED";
        return ResultOk;
    }
};

}
#endif /* PULSAR_AUTH_TLS_H_ */
