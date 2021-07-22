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
#include "pulsar/ProtobufNativeSchema.h"

#include <stdexcept>
#include <vector>

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <google/protobuf/descriptor.pb.h>

using google::protobuf::FileDescriptor;
using google::protobuf::FileDescriptorSet;

namespace pulsar {

void internalCollectFileDescriptors(const FileDescriptor* fileDescriptor,
                                    FileDescriptorSet& fileDescriptorSet);

SchemaInfo createProtobufNativeSchema(const google::protobuf::Descriptor* descriptor) {
    if (!descriptor) {
        throw std::invalid_argument("descriptor is null");
    }

    const auto fileDescriptor = descriptor->file();
    const std::string rootMessageTypeName = descriptor->full_name();
    const std::string rootFileDescriptorName = fileDescriptor->name();

    FileDescriptorSet fileDescriptorSet;
    internalCollectFileDescriptors(fileDescriptor, fileDescriptorSet);

    using namespace boost::archive::iterators;
    using base64 = base64_from_binary<transform_width<const char*, 6, 8>>;

    std::vector<char> bytes(fileDescriptorSet.ByteSizeLong());
    fileDescriptorSet.SerializeToArray(bytes.data(), bytes.size());

    const std::string schemaJson =
        R"({"fileDescriptorSet":")" + std::string(base64(bytes.data()), base64(bytes.data() + bytes.size())) +
        R"(","rootMessageTypeName":")" + rootMessageTypeName +
        R"(","rootFileDescriptorName":")" + rootFileDescriptorName + R"("})";

    return SchemaInfo(SchemaType::PROTOBUF_NATIVE, "", schemaJson);
}

void internalCollectFileDescriptors(const FileDescriptor* fileDescriptor,
                                    FileDescriptorSet& fileDescriptorSet) {
    fileDescriptor->CopyTo(fileDescriptorSet.add_file());
    for (int i = 0; i < fileDescriptor->dependency_count(); i++) {
        // collect the file descriptors recursively
        internalCollectFileDescriptors(fileDescriptor->dependency(i), fileDescriptorSet);
    }
}

}  // namespace pulsar
