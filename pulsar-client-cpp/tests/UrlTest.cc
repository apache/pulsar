/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Url.h"
#include <gtest/gtest.h>

using namespace pulsar;

TEST(UrlTest, testUrl) {
    Url url;

    ASSERT_TRUE(Url::parse("http://example.com", url));
    ASSERT_EQ("http", url.protocol());
    ASSERT_EQ(80, url.port());

    ASSERT_TRUE(Url::parse("https://example.com", url));
    ASSERT_EQ("https", url.protocol());
    ASSERT_EQ(443, url.port());

    ASSERT_TRUE(Url::parse("http://example.com:8080", url));
    ASSERT_EQ("http", url.protocol());
    ASSERT_EQ(8080, url.port());

    ASSERT_TRUE(Url::parse("http://example.com:8080/", url));
    ASSERT_EQ("http", url.protocol());
    ASSERT_EQ(8080, url.port());

    ASSERT_TRUE(Url::parse("http://example.com", url));
    ASSERT_EQ("http", url.protocol());
    ASSERT_EQ(80, url.port());

    ASSERT_TRUE(Url::parse("http://example.com:8080/test/my/path", url));
    ASSERT_EQ("http", url.protocol());
    ASSERT_EQ(8080, url.port());

    ASSERT_TRUE(Url::parse("http://example.com:8080/test/my/path?key=value#adsasda", url));
    ASSERT_EQ("http", url.protocol());
    ASSERT_EQ(8080, url.port());

    ASSERT_TRUE(Url::parse("pulsar://example.com:8080", url));
    ASSERT_EQ("pulsar", url.protocol());
    ASSERT_EQ(8080, url.port());

    ASSERT_TRUE(Url::parse("pulsar://example.com", url));
    ASSERT_EQ("pulsar", url.protocol());
    ASSERT_EQ(6650, url.port());
}
