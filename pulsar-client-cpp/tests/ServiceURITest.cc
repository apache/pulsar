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
#include <gtest/gtest.h>
#include "lib/ServiceURI.h"

using namespace pulsar;

static void verifyServiceURIFailure(const std::string& uriString, const std::string& errorMsg) {
    try {
        ServiceURI uri{uriString};
        std::cerr << uriString << " should be invalid" << std::endl;
        FAIL();
    } catch (const std::invalid_argument& e) {
        EXPECT_EQ(errorMsg, e.what());
    }
}

static void verifyServiceURI(const std::string& uriString, PulsarScheme expectedScheme,
                             const std::vector<std::string>& expectedServiceHosts) {
    ServiceURI uri{uriString};
    EXPECT_EQ(uri.getScheme(), expectedScheme);
    EXPECT_EQ(uri.getServiceHosts(), expectedServiceHosts);
}

TEST(ServiceURITest, testInvalidServiceUris) {
    verifyServiceURIFailure("localhost:6650", "The scheme part is missing: localhost:6650");
    verifyServiceURIFailure("unknown://localhost:6650", "Invalid scheme: unknown");
    verifyServiceURIFailure("://localhost:6650", "Expected scheme name at index 0: ://localhost:6650");
    verifyServiceURIFailure("pulsar:///", "authority component is missing in service uri: pulsar:///");
    verifyServiceURIFailure("pulsar://localhost:6650:6651", "invalid hostname: localhost:6650:6651");
    verifyServiceURIFailure("pulsar://localhost:xyz/", "invalid hostname: localhost:xyz");
    verifyServiceURIFailure("pulsar://localhost:-6650/", "invalid hostname: localhost:-6650");
}

TEST(ServiceURITest, testPathIgnored) {
    verifyServiceURI("pulsar://localhost:6650", PulsarScheme::PULSAR, {"pulsar://localhost:6650"});
    verifyServiceURI("pulsar://localhost:6650/", PulsarScheme::PULSAR, {"pulsar://localhost:6650"});
}

TEST(ServiceURITest, testMultipleHostsComma) {
    verifyServiceURI("pulsar://host1:6650,host2:6650,host3:6650/path/to/namespace", PulsarScheme::PULSAR,
                     {"pulsar://host1:6650", "pulsar://host2:6650", "pulsar://host3:6650"});
}

TEST(ServiceURITest, testMultipleHostsWithoutPulsarPorts) {
    verifyServiceURI("pulsar://host1,host2,host3/path/to/namespace", PulsarScheme::PULSAR,
                     {"pulsar://host1:6650", "pulsar://host2:6650", "pulsar://host3:6650"});
    verifyServiceURI("pulsar+ssl://host1,host2,host3/path/to/namespace", PulsarScheme::PULSAR_SSL,
                     {"pulsar+ssl://host1:6651", "pulsar+ssl://host2:6651", "pulsar+ssl://host3:6651"});
    verifyServiceURI("http://host1,host2,host3/path/to/namespace", PulsarScheme::HTTP,
                     {"http://host1:8080", "http://host2:8080", "http://host3:8080"});
    verifyServiceURI("https://host1,host2,host3/path/to/namespace", PulsarScheme::HTTPS,
                     {"https://host1:8081", "https://host2:8081", "https://host3:8081"});
}

TEST(ServiceURITest, testMultipleHostsMixed) {
    verifyServiceURI("pulsar://host1:6640,host2,host3:6660/path/to/namespace", PulsarScheme::PULSAR,
                     {"pulsar://host1:6640", "pulsar://host2:6650", "pulsar://host3:6660"});
}
