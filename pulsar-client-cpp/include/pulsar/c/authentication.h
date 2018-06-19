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

#ifdef __cplusplus
extern "C" {
#endif

#pragma GCC visibility push(default)

typedef struct _pulsar_authentication pulsar_authentication_t;

pulsar_authentication_t *pulsar_authentication_create(const char *dynamicLibPath,
                                                      const char *authParamsString);

pulsar_authentication_t *pulsar_authentication_tls_create(const char *certificatePath,
                                                          const char *privateKeyPath);

pulsar_authentication_t *pulsar_authentication_athenz_create(const char *authParamsString);

void pulsar_authentication_free(pulsar_authentication_t *authentication);

#pragma GCC visibility pop

#ifdef __cplusplus
}
#endif