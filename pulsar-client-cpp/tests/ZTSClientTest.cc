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
#include "lib/auth/athenz/ZTSClient.h"
#include <gtest/gtest.h>

using namespace pulsar;

namespace pulsar {

class ZTSClientWrapper {
   public:
    static PrivateKeyUri parseUri(const char* uri) { return ZTSClient::parseUri(uri); }
};
}  // namespace pulsar

TEST(ZTSClientTest, testZTSClient) {
    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("file:/path/to/private.key");
        ASSERT_EQ("file", uri.scheme);
        ASSERT_EQ("/path/to/private.key", uri.path);
    }

    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("file:///path/to/private.key");
        ASSERT_EQ("file", uri.scheme);
        ASSERT_EQ("/path/to/private.key", uri.path);
    }

    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("file:./path/to/private.key");
        ASSERT_EQ("file", uri.scheme);
        ASSERT_EQ("./path/to/private.key", uri.path);
    }

    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("file://./path/to/private.key");
        ASSERT_EQ("file", uri.scheme);
        ASSERT_EQ("./path/to/private.key", uri.path);
    }

    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("data:application/x-pem-file;base64,SGVsbG8gV29ybGQK");
        ASSERT_EQ("data", uri.scheme);
        ASSERT_EQ("application/x-pem-file;base64", uri.mediaTypeAndEncodingType);
        ASSERT_EQ("SGVsbG8gV29ybGQK", uri.data);
    }

    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("");
        ASSERT_EQ("", uri.scheme);
        ASSERT_EQ("", uri.path);
        ASSERT_EQ("", uri.mediaTypeAndEncodingType);
        ASSERT_EQ("", uri.data);
    }

    {
        PrivateKeyUri uri = ZTSClientWrapper::parseUri("/path/to/private.key");
        ASSERT_EQ("", uri.scheme);
        ASSERT_EQ("", uri.path);
        ASSERT_EQ("", uri.mediaTypeAndEncodingType);
        ASSERT_EQ("", uri.data);
    }
}
