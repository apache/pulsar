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
#include "utils.h"

AuthenticationWrapper::AuthenticationWrapper(const std::string& dynamicLibPath,
                                             const std::string& authParamsString) {
    this->auth = AuthFactory::create(dynamicLibPath, authParamsString);
}

struct AuthenticationTlsWrapper {
    AuthenticationPtr auth;

    AuthenticationTlsWrapper(const std::string& certificatePath, const std::string& privateKeyPath) {
        this->auth = AuthTls::create(certificatePath, privateKeyPath);
    }
};

struct AuthenticationAthenzWrapper {
    AuthenticationPtr auth;

    AuthenticationAthenzWrapper(const std::string& authParamsString) {
        this->auth = AuthAthenz::create(authParamsString);
    }
};

void export_authentication() {
    using namespace boost::python;

    class_<AuthenticationWrapper>("Authentication", init<const std::string&, const std::string&>())
            ;

    class_<AuthenticationTlsWrapper>("AuthenticationTLS", init<const std::string&, const std::string&>())
            ;

    class_<AuthenticationAthenzWrapper>("AuthenticationAthenz", init<const std::string&>())
            ;
}
