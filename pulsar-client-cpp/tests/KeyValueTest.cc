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
#include <pulsar/Client.h>

using namespace pulsar;

TEST(KeyValueTest, testEncodeAndDeCode) {
    // test encode
    std::string keyContent = "keyContent";
    std::string valueContent = "valueContent";
    KeyValue keyValue(keyContent, valueContent, KeyValueEncodingType::INLINE);
    ASSERT_EQ(keyValue.getContent().size(), 8 + keyContent.size() + valueContent.size());

    // test decode
    KeyValue deCodeKeyValue(keyValue.getContent(), KeyValueEncodingType::INLINE);
    ASSERT_EQ(deCodeKeyValue.getKeyData(), keyContent);
    ASSERT_TRUE(deCodeKeyValue.getContent().compare(valueContent) != 0);

    // test separated type
    KeyValue sepKeyValue(keyContent, valueContent, KeyValueEncodingType::SEPARATED);
    ASSERT_EQ(sepKeyValue.getKeyData(), keyContent);
    ASSERT_EQ(sepKeyValue.getContent(), valueContent);
}

TEST(KeyValueTest, testKeyIsEmpty) {
    // test encode
    std::string keyContent;
    std::string valueContent = "valueContent";
    KeyValue keyValue(keyContent, valueContent, KeyValueEncodingType::INLINE);
    ASSERT_EQ(keyValue.getContent().size(), 8 + keyContent.size() + valueContent.size());

    // test decode
    KeyValue deCodeKeyValue(keyValue.getContent(), KeyValueEncodingType::INLINE);
    ASSERT_EQ(deCodeKeyValue.getKeyData(), keyContent);
    ASSERT_TRUE(deCodeKeyValue.getContent().compare(valueContent) != 0);

    // test separated type
    KeyValue sepKeyValue(keyContent, valueContent, KeyValueEncodingType::SEPARATED);
    ASSERT_EQ(sepKeyValue.getKeyData(), keyContent);
    ASSERT_EQ(sepKeyValue.getContent(), valueContent);
}

TEST(KeyValueTest, testValueIsEmpty) {
    // test encode
    std::string keyContent = "keyContent";
    std::string valueContent;
    KeyValue keyValue(keyContent, valueContent, KeyValueEncodingType::INLINE);
    ASSERT_EQ(keyValue.getContent().size(), 8 + keyContent.size() + valueContent.size());

    // test decode
    KeyValue deCodeKeyValue(keyValue.getContent(), KeyValueEncodingType::INLINE);
    ASSERT_EQ(deCodeKeyValue.getKeyData(), keyContent);
    ASSERT_TRUE(deCodeKeyValue.getContent().compare(valueContent) != 0);

    // test separated type
    KeyValue sepKeyValue(keyContent, valueContent, KeyValueEncodingType::SEPARATED);
    ASSERT_EQ(sepKeyValue.getKeyData(), keyContent);
    ASSERT_EQ(sepKeyValue.getContent(), valueContent);
}
