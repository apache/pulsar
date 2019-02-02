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

#include <pulsar/c/authentication.h>

#include <pulsar/Authentication.h>

#include "c_structs.h"

#include <cstdlib>

pulsar_authentication_t *pulsar_authentication_create(const char *dynamicLibPath,
                                                      const char *authParamsString) {
    pulsar_authentication_t *authentication = new pulsar_authentication_t;
    authentication->auth = pulsar::AuthFactory::create(dynamicLibPath, authParamsString);
    return authentication;
}

void pulsar_authentication_free(pulsar_authentication_t *authentication) { delete authentication; }

pulsar_authentication_t *pulsar_authentication_tls_create(const char *certificatePath,
                                                          const char *privateKeyPath) {
    pulsar_authentication_t *authentication = new pulsar_authentication_t;
    authentication->auth = pulsar::AuthTls::create(certificatePath, privateKeyPath);
    return authentication;
}

pulsar_authentication_t *pulsar_authentication_athenz_create(const char *authParamsString) {
    pulsar_authentication_t *authentication = new pulsar_authentication_t;
    authentication->auth = pulsar::AuthAthenz::create(authParamsString);
    return authentication;
}

pulsar_authentication_t *pulsar_authentication_token_create(const char *token) {
    pulsar_authentication_t *authentication = new pulsar_authentication_t;
    authentication->auth = pulsar::AuthToken::createWithToken(token);
    return authentication;
}

static std::string tokenSupplierWrapper(token_supplier supplier, void *ctx) {
    const char *token = supplier(ctx);
    std::string tokenStr = token;
    free((void *)token);
    return tokenStr;
}

pulsar_authentication_t *pulsar_authentication_token_create_with_supplier(token_supplier tokenSupplier,
                                                                          void *ctx) {
    pulsar_authentication_t *authentication = new pulsar_authentication_t;
    authentication->auth = pulsar::AuthToken::create(std::bind(&tokenSupplierWrapper, tokenSupplier, ctx));
    return authentication;
}