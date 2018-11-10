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

#include <pulsar/c/string_list.h>

#include "c_structs.h"

pulsar_string_list_t *pulsar_string_list_create() { return new pulsar_string_list_t; }

void pulsar_string_list_free(pulsar_string_list_t *list) { delete list; }

int pulsar_string_list_size(pulsar_string_list_t *list) { return list->list.size(); }

void pulsar_string_list_append(pulsar_string_list_t *list, const char *item) { list->list.push_back(item); }

const char *pulsar_string_list_get(pulsar_string_list_t *list, int index) {
    return list->list[index].c_str();
}
