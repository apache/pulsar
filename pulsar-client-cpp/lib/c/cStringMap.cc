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

#include <pulsar/c/string_map.h>

#include "c_structs.h"

pulsar_string_map_t *pulsar_string_map_create() { return new pulsar_string_map_t; }

void pulsar_string_map_free(pulsar_string_map_t *map) { delete map; }

int pulsar_string_map_size(pulsar_string_map_t *map) { return map->map.size(); }

void pulsar_string_map_put(pulsar_string_map_t *map, const char *key, const char *value) {
    map->map[key] = value;
}

const char *pulsar_string_map_get(pulsar_string_map_t *map, const char *key) {
    std::map<std::string, std::string>::iterator it = map->map.find(key);

    if (it == map->map.end()) {
        return NULL;
    } else {
        return it->second.c_str();
    }
}

const char *pulsar_string_map_get_key(pulsar_string_map_t *map, int idx) {
    std::map<std::string, std::string>::iterator it = map->map.begin();
    while (idx-- > 0) {
        ++it;
    }

    return it->first.c_str();
}

const char *pulsar_string_map_get_value(pulsar_string_map_t *map, int idx) {
    std::map<std::string, std::string>::iterator it = map->map.begin();
    while (idx-- > 0) {
        ++it;
    }

    return it->second.c_str();
}