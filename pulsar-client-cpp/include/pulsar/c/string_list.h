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

typedef struct _pulsar_string_list pulsar_string_list_t;

pulsar_string_list_t *pulsar_string_list_create();
void pulsar_string_list_free(pulsar_string_list_t *list);

int pulsar_string_list_size(pulsar_string_list_t *list);

void pulsar_string_list_append(pulsar_string_list_t *list, const char *item);

const char *pulsar_string_list_get(pulsar_string_list_t *map, int index);

#pragma GCC visibility pop

#ifdef __cplusplus
}
#endif