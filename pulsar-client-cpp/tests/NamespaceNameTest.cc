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
#include <NamespaceName.h>

#include <gtest/gtest.h>
using namespace pulsar;

TEST(NamespaceNameTest, testNamespaceName) {
    std::shared_ptr<NamespaceName> nn1 = NamespaceName::get("property", "cluster", "namespace");
    ASSERT_EQ("property", nn1->getProperty());
    ASSERT_EQ("cluster", nn1->getCluster());
    ASSERT_EQ("namespace", nn1->getLocalName());
    ASSERT_FALSE(nn1->isV2());

    std::shared_ptr<NamespaceName> nn2 = NamespaceName::get("property", "cluster", "namespace");
    ASSERT_TRUE(*nn1 == *nn2);
}

TEST(NamespaceNameTest, testNamespaceNameV2) {
    std::shared_ptr<NamespaceName> nn1 = NamespaceName::get("property", "namespace");
    ASSERT_EQ("property", nn1->getProperty());
    ASSERT_TRUE(nn1->getCluster().empty());
    ASSERT_EQ("namespace", nn1->getLocalName());
    ASSERT_TRUE(nn1->isV2());

    std::shared_ptr<NamespaceName> nn2 = NamespaceName::get("property", "namespace");
    ASSERT_TRUE(*nn1 == *nn2);
}
