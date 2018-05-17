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
#include <pulsar/Client.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <boost/functional/hash.hpp>

#include "../lib/BoostHash.h"
#include "../lib/JavaStringHash.h"
#include "../lib/Murmur3_32Hash.h"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::ReturnRef;

using namespace pulsar;

TEST(HashTest, testBoostHash) {
    BoostHash hash;
    boost::hash<std::string> boostHash;

    std::string key1 = "key1";
    std::string key2 = "key2";

    ASSERT_EQ(boostHash(key1) & std::numeric_limits<int32_t>::max(), hash.makeHash(key1));
    ASSERT_EQ(boostHash(key2) & std::numeric_limits<int32_t>::max(), hash.makeHash(key2));
}

TEST(HashTest, testJavaStringHash) {
    JavaStringHash hash;

    // Calculating `hashCode()` makes overflow as unsigned int32.
    std::string key1 = "keykeykeykeykey1";

    // `hashCode()` is negative as signed int32.
    std::string key2 = "keykeykey2";

    // Same as Java client
    ASSERT_EQ(434058482, hash.makeHash(key1));
    ASSERT_EQ(42978643, hash.makeHash(key2));
}

TEST(HashTest, testMurmur3_32Hash) {
    Murmur3_32Hash hash;
    std::string k1 = "k1";
    std::string k2 = "k2";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string key01 = "key01";
    std::string key02 = "key02";

    // Same value as Java client
    ASSERT_EQ(2110152746, hash.makeHash(k1));
    ASSERT_EQ(1479966664, hash.makeHash(k2));
    ASSERT_EQ(462881061, hash.makeHash(key1));
    ASSERT_EQ(1936800180, hash.makeHash(key2));
    ASSERT_EQ(39696932, hash.makeHash(key01));
    ASSERT_EQ(751761803, hash.makeHash(key02));
}
