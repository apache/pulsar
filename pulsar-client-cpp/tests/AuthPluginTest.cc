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

#include "pulsar/Auth.h"
#include <gtest/gtest.h>

TEST(AuthPluginTest, testCreate) {
    std::string data;

    pulsar::AuthenticationPtr auth = pulsar::Auth::create("../lib/auth/libauthtls.so");
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data, "THIS_SHOULD_BE_REPLACED");
    ASSERT_EQ(auth.use_count(), 1);
}

TEST(AuthPluginTest, testDisable) {
    std::string data;

    pulsar::AuthenticationPtr auth = pulsar::Auth::Disabled();
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "none");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data, "");
	ASSERT_EQ(auth.use_count(), 1);
}
