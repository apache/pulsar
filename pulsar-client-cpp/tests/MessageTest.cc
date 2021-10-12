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
#include <pulsar/MessageBuilder.h>
#include <pulsar/Client.h>
#include <gtest/gtest.h>
#include <string>
#include <lib/LogUtils.h>

using namespace pulsar;
TEST(MessageTest, testMessageContents) {
    MessageBuilder msgBuilder1;
    std::string content = "my-content";
    msgBuilder1.setContent(content);
    Message msg = msgBuilder1.build();
    ASSERT_EQ(content, msg.getDataAsString());
    ASSERT_EQ(content.length(), msg.getLength());
    ASSERT_EQ(content, std::string((char*)msg.getData(), msg.getLength()));

    MessageBuilder msgBuilder2;
    std::string myContents = "mycontents";
    msgBuilder2.setContent(myContents.c_str(), myContents.length());
    msg = msgBuilder2.build();
    ASSERT_EQ(myContents, std::string((char*)msg.getData(), msg.getLength()));
    ASSERT_NE(myContents.c_str(), (char*)msg.getData());
    ASSERT_EQ(myContents, msg.getDataAsString());
    ASSERT_EQ(std::string("mycontents").length(), msg.getLength());
}

TEST(MessageTest, testAllocatedContents) {
    MessageBuilder msgBuilder;
    std::string str = "content";
    char* content = new char[str.length() + 1];
    strncpy(content, str.c_str(), str.length());
    msgBuilder.setAllocatedContent(content, str.length());
    Message msg = msgBuilder.build();
    ASSERT_FALSE(strncmp("content", (char*)msg.getData(), msg.getLength()));
    ASSERT_EQ(content, (char*)msg.getData());
    delete[] content;
}

template <typename Map>
bool compareMaps(const Map& lhs, const Map& rhs) {
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

TEST(MessageTest, testProperties) {
    MessageBuilder msgBuilder1;
    msgBuilder1.setProperty("property1", "value1");
    Message msg = msgBuilder1.build();
    ASSERT_EQ(msg.getProperty("property1"), "value1");

    MessageBuilder msgBuilder2;
    Message::StringMap stringMap;
    stringMap.insert(std::pair<std::string, std::string>("p1", "v1"));
    stringMap.insert(std::pair<std::string, std::string>("p2", "v2"));
    stringMap.insert(std::pair<std::string, std::string>("p3", "v3"));
    msgBuilder2.setProperties(stringMap);
    msg = msgBuilder2.build();
    ASSERT_EQ(msg.getProperty("p1"), "v1");
    ASSERT_EQ(msg.getProperty("p2"), "v2");
    ASSERT_EQ(msg.getProperty("p3"), "v3");
    ASSERT_TRUE(compareMaps(msg.getProperties(), stringMap));
}
