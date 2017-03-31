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

#include <ClientConnectionContainer.h>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <gtest/gtest.h>
#include <lib/LogUtils.h>
DECLARE_LOG_OBJECT();

using namespace pulsar;

TEST(ClientConnectionContainerTest, basicWorking) {
    int nextValue;
    // Initializing a random variable
    srand(time(NULL));

    // Initialize a container of size 3
    ClientConnectionContainer<int> container(3);
    // Successfully add 3 elements
    int obj = 0;
    ASSERT_TRUE(container.add(obj));
    obj = 1;
    ASSERT_TRUE(container.add(obj));
    ASSERT_FALSE(container.isFull());
    obj = 2;
    ASSERT_TRUE(container.add(obj));
    ASSERT_TRUE(container.isFull());

    // Fail on trying to add the fourth element
    obj = 3;
    ASSERT_FALSE(container.add(obj));
    ASSERT_EQ(container.size(), 3);
    ASSERT_TRUE(container.isFull());

    // random number [1 100]
    int rnumber = rand() % 100 + 1;
    LOG_DEBUG("rnumber = " << rnumber);

    // Test wrap around functionality
    for (int i = 0; i < rnumber; i++) {
        ASSERT_TRUE(container.getNext(nextValue));
        ASSERT_EQ(nextValue, i % 3);
    }

    // Test remove functionality
    int sum = 0;
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(container.getNext(nextValue));
        sum += nextValue;
        ASSERT_TRUE(container.remove());
        ASSERT_EQ(container.size(), 3 - (i + 1));
        ASSERT_FALSE(container.isFull());
    }
    // 3 = 0 + 1 + 2
    ASSERT_EQ(sum, 3);
    ASSERT_FALSE(container.remove());
    ASSERT_FALSE(container.getNext(nextValue));
}

TEST(ClientConnectionContainerTest, removeOperation) {
    int nextValue;
    // Initialize a container of size 3
    ClientConnectionContainer<int> container(5);

    // Successfully add 2 elements
    int obj = 0;
    ASSERT_TRUE(container.add(obj));
    obj = 1;
    ASSERT_TRUE(container.add(obj));

    // [0 1] - remove before getNext() - hence oldest(0) removed
    ASSERT_TRUE(container.remove());
    ASSERT_EQ(container.size(), 1);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 1);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 1);

    // [1 2 3] - adding
    obj = 2;
    ASSERT_TRUE(container.add(obj));
    obj = 3;
    ASSERT_TRUE(container.add(obj));

    // [1 2 3] - last getNext returned 1 hence 1 deleted
    ASSERT_TRUE(container.remove());

    // [2 3]
    // jumps over an element if the element returned by previous call is deleted
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 2);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 3);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 3);

    // [2 3] - last getNext returned 2 hence 2 deleted
    ASSERT_TRUE(container.remove());
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 3);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 3);
    ASSERT_TRUE(container.remove());

    // empty list
    ASSERT_FALSE(container.remove());

    // [4 5] - adding
    obj = 4;
    ASSERT_TRUE(container.add(obj));
    obj = 5;
    ASSERT_TRUE(container.add(obj));

    // since list became empty - we start deleting from start (4)
    ASSERT_TRUE(container.remove());

    // [5]
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 5);
    obj = 6;
    ASSERT_TRUE(container.add(obj));
    obj = 7;
    ASSERT_TRUE(container.add(obj));
    obj = 8;
    ASSERT_TRUE(container.add(obj));
    obj = 8;
    ASSERT_TRUE(container.add(obj));

    // [5 6 7 8]
    ASSERT_TRUE(container.remove());  // 5 removed
    // 9 removed - removes element in reverse order starting from the one returned by last getNext() call
    ASSERT_TRUE(container.remove());
    // 8 removed - removes element in reverse order starting from the one returned by last getNext() call
    ASSERT_TRUE(container.remove());
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 6);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 7);
    ASSERT_TRUE(container.remove());  // 7 removed
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 6);
    ASSERT_TRUE(container.remove());  // 6 removed

    ASSERT_FALSE(container.getNext(nextValue));
}

TEST(ClientConnectionContainerTest, negativeTests) {
    int nextValue;
    ClientConnectionContainer<int> container(3);
    ASSERT_FALSE(container.getNext(nextValue));

    ClientConnectionContainer<int> container(0);
    ASSERT_EQ(container.size(), 0);
    ASSERT_EQ(container.capacity(), 2);
    ASSERT_TRUE(container.isEmpty());
    ASSERT_FALSE(container.isFull());
}

TEST(ClientConnectionContainerTest, addOperation) {
    int nextValue;
    // Initialize a container of size 3
    ClientConnectionContainer<int> container(1);
    // Successfully add 2 elements
    int obj = 0;
    ASSERT_TRUE(container.add(obj));
    obj = 1;
    ASSERT_FALSE(container.add(obj));

    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 0);
    ASSERT_TRUE(container.getNext(nextValue));
    ASSERT_EQ(nextValue, 0);

    ASSERT_TRUE(container.remove());
    ASSERT_FALSE(container.isFull());
    ASSERT_EQ(container.size(), 0);
}

