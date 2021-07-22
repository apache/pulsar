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

#include <pulsar/Schema.h>
#include <google/protobuf/descriptor.h>

namespace pulsar {

/**
 * Create a protobuf native schema using a descriptor.
 *
 * @param descriptor the Descriptor object of the target class
 * @return the protobuf native schema
 * @throw std::invalid_argument if descriptor is nullptr
 */
PULSAR_PUBLIC SchemaInfo createProtobufNativeSchema(const google::protobuf::Descriptor* descriptor);

}  // namespace pulsar
