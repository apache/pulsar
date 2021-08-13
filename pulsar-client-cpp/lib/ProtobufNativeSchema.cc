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

    std::string base64String{base64(bytes.data()), base64(bytes.data() + bytes.size())};
    // Pulsar broker only supports decoding Base64 with padding so we need to add padding '=' here
    const size_t numPadding = 4 - base64String.size() % 4;
    if (numPadding <= 2) {
        for (size_t i = 0; i < numPadding; i++) {
            base64String.push_back('=');
        }
    } else if (numPadding == 3) {
        // The length of encoded Base64 string (without padding) should not be 4N+1
        throw std::runtime_error("Unexpected padding number (3), the encoded Base64 string is:\n" +
                                 base64String);
    }  // else numPadding == 4, which means no padding characters need to be added

    const std::string schemaJson = R"({"fileDescriptorSet":")" + base64String +
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
