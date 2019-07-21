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
#include <lib/BlockingQueue.h>

#include <thread>

class ProducerWorker {
   private:
    std::thread producerThread_;
    BlockingQueue<int>& queue_;

   public:
    ProducerWorker(BlockingQueue<int>& queue) : queue_(queue) {}

    void produce(int number) { producerThread_ = std::thread(&ProducerWorker::pushNumbers, this, number); }

    void pushNumbers(int number) {
        for (int i = 1; i <= number; i++) {
            queue_.push(i);
        }
    }

    void join() { producerThread_.join(); }
};

class ConsumerWorker {
   private:
    std::thread consumerThread_;
    BlockingQueue<int>& queue_;

   public:
    ConsumerWorker(BlockingQueue<int>& queue) : queue_(queue) {}

    void consume(int number) { consumerThread_ = std::thread(&ConsumerWorker::popNumbers, this, number); }

    void popNumbers(int number) {
        for (int i = 1; i <= number; i++) {
            int poppedElement;
            queue_.pop(poppedElement);
        }
    }

    void join() { consumerThread_.join(); }
};

TEST(BlockingQueueTest, testBasic) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    ProducerWorker producerWorker(queue);
    producerWorker.produce(5);

    ConsumerWorker consumerWorker(queue);
    consumerWorker.consume(5);

    producerWorker.join();
    consumerWorker.join();

    size_t zero = 0;
    ASSERT_EQ(zero, queue.size());
}

TEST(BlockingQueueTest, testQueueOperations) {
    size_t size = 5;
    BlockingQueue<int> queue(size);
    for (size_t i = 1; i <= size; i++) {
        queue.push(i);
    }
    ASSERT_EQ(queue.size(), size);

    int cnt = 1;
    for (BlockingQueue<int>::const_iterator it = queue.begin(); it != queue.end(); it++) {
        ASSERT_EQ(cnt, *it);
        ++cnt;
    }

    cnt = 1;
    for (BlockingQueue<int>::iterator it = queue.begin(); it != queue.end(); it++) {
        ASSERT_EQ(cnt, *it);
        ++cnt;
    }

    int poppedElement;
    for (size_t i = 1; i <= size; i++) {
        queue.pop(poppedElement);
    }

    ASSERT_FALSE(queue.peek(poppedElement));
}

TEST(BlockingQueueTest, testBlockingProducer) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    ProducerWorker producerWorker(queue);
    producerWorker.produce(8);

    ConsumerWorker consumerWorker(queue);
    consumerWorker.consume(5);

    producerWorker.join();
    consumerWorker.join();

    size_t three = 3;
    ASSERT_EQ(three, queue.size());
}

TEST(BlockingQueueTest, testBlockingConsumer) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    ProducerWorker producerWorker(queue);
    producerWorker.produce(5);

    ConsumerWorker consumerWorker(queue);
    consumerWorker.consume(8);

    producerWorker.pushNumbers(3);

    producerWorker.join();
    consumerWorker.join();

    size_t zero = 0;
    ASSERT_EQ(zero, queue.size());
}

TEST(BlockingQueueTest, testTimeout) {
    size_t size = 5;
    BlockingQueue<int> queue(size);
    int value;
    bool popReturn = queue.pop(value, std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_FALSE(popReturn);
}

TEST(BlockingQueueTest, testReservedSpot) {
    size_t size = 3;
    BlockingQueue<int> queue(size);

    ASSERT_TRUE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(0, queue.size());

    queue.push(1);
    ASSERT_FALSE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(1, queue.size());

    BlockingQueue<int>::ReservedSpot spot1 = queue.reserve();
    ASSERT_FALSE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(1, queue.size());

    queue.push(2, spot1);

    ASSERT_FALSE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(2, queue.size());

    {
        BlockingQueue<int>::ReservedSpot spot2 = queue.reserve();

        ASSERT_FALSE(queue.empty());
        ASSERT_TRUE(queue.full());
        ASSERT_EQ(2, queue.size());
    }

    ASSERT_FALSE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(2, queue.size());

    BlockingQueue<int>::ReservedSpot spot3 = queue.reserve();

    int res;
    queue.pop(res);
    ASSERT_EQ(1, res);

    ASSERT_FALSE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(1, queue.size());

    queue.pop(res);
    ASSERT_EQ(2, res);

    ASSERT_TRUE(queue.empty());
    ASSERT_FALSE(queue.full());
    ASSERT_EQ(0, queue.size());

    spot3.release();

    {
        BlockingQueue<int>::ReservedSpot spot1 = queue.reserve();
        BlockingQueue<int>::ReservedSpot spot2 = queue.reserve();
        BlockingQueue<int>::ReservedSpot spot3 = queue.reserve();

        ASSERT_TRUE(queue.empty());
        ASSERT_TRUE(queue.full());
        ASSERT_EQ(0, queue.size());
    }
}
