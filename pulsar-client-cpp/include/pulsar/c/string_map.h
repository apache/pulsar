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

typedef struct _pulsar_string_map pulsar_string_map_t;

pulsar_string_map_t *pulsar_string_map_create();
void pulsar_string_map_free(pulsar_string_map_t *map);

int pulsar_string_map_size(pulsar_string_map_t *map);

void pulsar_string_map_put(pulsar_string_map_t *map, const char *key, const char *value);

const char *pulsar_string_map_get(pulsar_string_map_t *map, const char *key);

const char *pulsar_string_map_get_key(pulsar_string_map_t *map, int idx);
const char *pulsar_string_map_get_value(pulsar_string_map_t *map, int idx);

#pragma GCC visibility pop

#ifdef __cplusplus
}
#endif