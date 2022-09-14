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
#include <KeyValueImpl.h>

using namespace pulsar;

TEST(KeyValueTest, testEncodeAndDeCode) {
    const std::string keyContent = "keyContent";
    const std::string valueContent = "valueContent";

    {
        // test inline encode
        KeyValueImpl keyValue((std::string(keyContent)), std::string(valueContent));
        const SharedBuffer content = keyValue.getContent(KeyValueEncodingType::INLINE);
        ASSERT_EQ(content.readableBytes(), 8 + keyContent.size() + valueContent.size());

        // test inline decode
        KeyValueImpl deCodeKeyValue(content.data(), content.readableBytes(), KeyValueEncodingType::INLINE);
        const SharedBuffer deCodeContent = deCodeKeyValue.getContent(KeyValueEncodingType::INLINE);
        ASSERT_EQ(deCodeKeyValue.getKey(), keyContent);
        ASSERT_EQ(deCodeKeyValue.getValueAsString(), valueContent);
        ASSERT_TRUE(std::string(deCodeContent.data(), deCodeContent.readableBytes()).compare(valueContent) !=
                    0);
    }

    {
        // test separated encode
        KeyValueImpl sepKeyValue((std::string(keyContent)), std::string(valueContent));
        const SharedBuffer content = sepKeyValue.getContent(KeyValueEncodingType::SEPARATED);
        ASSERT_EQ(sepKeyValue.getKey(), keyContent);
        ASSERT_EQ(sepKeyValue.getValueAsString(), valueContent);
        ASSERT_EQ(std::string(content.data(), content.readableBytes()), valueContent);

        // test separated decode
        KeyValueImpl sepDeKeyValue(content.data(), content.readableBytes(), KeyValueEncodingType::SEPARATED);
        const SharedBuffer deCodeContent = sepKeyValue.getContent(KeyValueEncodingType::SEPARATED);
        ASSERT_EQ(sepDeKeyValue.getKey(), "");
        ASSERT_EQ(sepDeKeyValue.getValueAsString(), valueContent);
        ASSERT_EQ(std::string(deCodeContent.data(), deCodeContent.readableBytes()), valueContent);
    }
}

TEST(KeyValueTest, testKeyIsEmpty) {
    const std::string keyContent;
    const std::string valueContent = "valueContent";

    {
        // test inline encode
        KeyValueImpl keyValue((std::string(keyContent)), std::string(valueContent));
        const SharedBuffer content = keyValue.getContent(KeyValueEncodingType::INLINE);
        ASSERT_EQ(content.readableBytes(), 8 + keyContent.size() + valueContent.size());

        // test inline decode
        KeyValueImpl deCodeKeyValue(content.data(), content.readableBytes(), KeyValueEncodingType::INLINE);
        const SharedBuffer deCodeContent = deCodeKeyValue.getContent(KeyValueEncodingType::INLINE);
        ASSERT_EQ(deCodeKeyValue.getKey(), keyContent);
        ASSERT_EQ(deCodeKeyValue.getValueAsString(), valueContent);
        ASSERT_TRUE(std::string(deCodeContent.data(), deCodeContent.readableBytes()).compare(valueContent) !=
                    0);
    }

    {
        // test separated type
        KeyValueImpl sepKeyValue((std::string(keyContent)), std::string(valueContent));
        const SharedBuffer content = sepKeyValue.getContent(KeyValueEncodingType::SEPARATED);
        ASSERT_EQ(sepKeyValue.getKey(), keyContent);
        ASSERT_EQ(sepKeyValue.getValueAsString(), valueContent);
        ASSERT_EQ(std::string(content.data(), content.readableBytes()), valueContent);
    }
}

TEST(KeyValueTest, testValueIsEmpty) {
    const std::string keyContent = "keyContent";
    const std::string valueContent;

    {
        // test inline encode
        KeyValueImpl keyValue((std::string(keyContent)), std::string(valueContent));
        const SharedBuffer content = keyValue.getContent(KeyValueEncodingType::INLINE);
        ASSERT_EQ(content.readableBytes(), 8 + keyContent.size() + valueContent.size());

        // test inline decode
        KeyValueImpl deCodeKeyValue(content.data(), content.readableBytes(), KeyValueEncodingType::INLINE);
        const SharedBuffer deCodeContent = keyValue.getContent(KeyValueEncodingType::INLINE);
        ASSERT_EQ(deCodeKeyValue.getKey(), keyContent);
        ASSERT_EQ(deCodeKeyValue.getValueAsString(), valueContent);
        ASSERT_TRUE(std::string(deCodeContent.data(), deCodeContent.readableBytes()).compare(valueContent) !=
                    0);
    }

    {
        // test separated type
        KeyValueImpl sepKeyValue((std::string(keyContent)), std::string(valueContent));
        const SharedBuffer content = sepKeyValue.getContent(KeyValueEncodingType::SEPARATED);
        ASSERT_EQ(sepKeyValue.getKey(), keyContent);
        ASSERT_EQ(sepKeyValue.getValueAsString(), valueContent);
        ASSERT_EQ(std::string(content.data(), content.readableBytes()), valueContent);
    }
}
