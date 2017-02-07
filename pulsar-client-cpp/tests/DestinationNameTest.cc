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

#include <DestinationName.h>

#include <gtest/gtest.h>

using namespace pulsar;

TEST(DestinationNameTest, testLookup) {
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
            "persistent://pulsar/bf1/TESTNS.0/curveballapps");
    std::string lookup_name = dn->getLookupName();
    ASSERT_EQ(lookup_name, "persistent/pulsar/bf1/TESTNS.0/curveballapps");
}

TEST(DestinationNameTest, testDestinationName) {
    // Compare getters and setters
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
            "persistent://property/cluster/namespace/destination");
    ASSERT_EQ("property", dn->getProperty());
    ASSERT_EQ("cluster", dn->getCluster());
    ASSERT_EQ("namespace", dn->getNamespacePortion());
    ASSERT_EQ("persistent", dn->getDomain());
    ASSERT_EQ(DestinationName::getEncodedName("destination"), dn->getLocalName());

    // Compare == operator
    boost::shared_ptr<DestinationName> dn1 = DestinationName::get("persistent://p/c/n/d");
    boost::shared_ptr<DestinationName> dn2 = DestinationName::get("persistent://p/c/n/d");
    ASSERT_TRUE(*dn1 == *dn2);
}

TEST(DestinationNameTest, testDestinationNameWithSlashes) {
    // Compare getters and setters
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
            "persistent://property/cluster/namespace/destination/name/with/slash");
    ASSERT_EQ("property", dn->getProperty());
    ASSERT_EQ("cluster", dn->getCluster());
    ASSERT_EQ("namespace", dn->getNamespacePortion());
    ASSERT_EQ("persistent", dn->getDomain());
    ASSERT_EQ("destination/name/with/slash", dn->getLocalName());

    dn = DestinationName::get("persistent://property/cluster/namespace/destination/ends/with/slash/");
    ASSERT_TRUE(dn != NULL);
    ASSERT_EQ(DestinationName::getEncodedName("destination/ends/with/slash/"), dn->getEncodedLocalName());

    dn = DestinationName::get("persistent://property/cluster/namespace/`~!@#$%^&*()-_+=[]{}|\\;:'\"<>,./?");
    ASSERT_TRUE(dn != NULL);
    ASSERT_EQ(DestinationName::getEncodedName("`~!@#$%^&*()-_+=[]{}|\\;:'\"<>,./?"), dn->getEncodedLocalName());

    dn = DestinationName::get("persistent://property/cluster/namespace/topic@%*)(&!%$#@#$><?");
    ASSERT_TRUE(dn != NULL);
    ASSERT_EQ(DestinationName::getEncodedName("topic@%*)(&!%$#@#$><?"), dn->getEncodedLocalName());

    dn = DestinationName::get("persistent://property/cluster/namespace/destination//with//double//slash//");
    ASSERT_TRUE(dn != NULL);
    ASSERT_EQ(DestinationName::getEncodedName("destination//with//double//slash//"), dn->getEncodedLocalName());

    dn = DestinationName::get("persistent://property/cluster/namespace//destination/starts/with/slash/");
    ASSERT_TRUE(dn != NULL);
    ASSERT_EQ(DestinationName::getEncodedName("/destination/starts/with/slash/"), dn->getEncodedLocalName());

}
TEST(DestinationNameTest, testEmptyClusterName) {
    // Compare getters and setters
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
            "persistent://property//namespace/destination");

   ASSERT_FALSE(dn);
}

TEST(DestinationNameTest, testExtraSlashes) {
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
            "persistent://property/cluster//namespace/destination");
    ASSERT_FALSE(dn);
    dn = DestinationName::get("persistent://property//cluster//namespace//destination");
    ASSERT_FALSE(dn);
}

TEST(DestinationNameTest, testIllegalCharacters) {
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
               "persistent://prop!!!erty/cluster&)&Name/name%%%space/destination");
    ASSERT_FALSE(dn);
}

TEST(DestinationNameTest, testIllegalUrl) {
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
               "persistent:::/property/cluster/namespace/destination");
    ASSERT_FALSE(dn);
}

TEST(DestinationNameTest, testEmptyString) {
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
               "");
    ASSERT_FALSE(dn);
}

TEST(DestinationNameTest, testExtraArguments) {
    boost::shared_ptr<DestinationName> dn = DestinationName::get(
               "persistent:::/property/cluster/namespace/destination/some/extra/args");
    ASSERT_FALSE(dn);
}
