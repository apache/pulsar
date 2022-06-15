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
#include <lib/UnboundedBlockingQueue.h>
#include <lib/Latch.h>

#include <future>
#include <thread>

class UnboundedProducerWorker {
   private:
    std::thread producerThread_;
    UnboundedBlockingQueue<int>& queue_;

   public:
    UnboundedProducerWorker(UnboundedBlockingQueue<int>& queue) : queue_(queue) {}

    void produce(int number) {
        producerThread_ = std::thread(&UnboundedProducerWorker::pushNumbers, this, number);
    }

    void pushNumbers(int number) {
        for (int i = 1; i <= number; i++) {
            queue_.push(i);
        }
    }

    void join() { producerThread_.join(); }
};

class UnboundedConsumerWorker {
   private:
    std::thread consumerThread_;
    UnboundedBlockingQueue<int>& queue_;

   public:
    UnboundedConsumerWorker(UnboundedBlockingQueue<int>& queue) : queue_(queue) {}

    void consume(int number) {
        consumerThread_ = std::thread(&UnboundedConsumerWorker::popNumbers, this, number);
    }

    void popNumbers(int number) {
        for (int i = 1; i <= number; i++) {
            int poppedElement;
            queue_.pop(poppedElement);
        }
    }

    void join() { consumerThread_.join(); }
};

TEST(UnboundedBlockingQueueTest, testBasic) {
    size_t size = 5;
    UnboundedBlockingQueue<int> queue(size);

    UnboundedProducerWorker producerWorker(queue);
    producerWorker.produce(5);

    UnboundedConsumerWorker consumerWorker(queue);
    consumerWorker.consume(5);

    producerWorker.join();
    consumerWorker.join();

    size_t zero = 0;
    ASSERT_EQ(zero, queue.size());
}

TEST(UnboundedBlockingQueueTest, testQueueOperations) {
    size_t size = 5;
    UnboundedBlockingQueue<int> queue(size);
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

TEST(UnboundedBlockingQueueTest, testBlockingProducer) {
    size_t size = 5;
    UnboundedBlockingQueue<int> queue(size);

    UnboundedProducerWorker producerWorker(queue);
    producerWorker.produce(8);

    UnboundedConsumerWorker consumerWorker(queue);
    consumerWorker.consume(5);

    producerWorker.join();
    consumerWorker.join();

    size_t three = 3;
    ASSERT_EQ(three, queue.size());
}

TEST(UnboundedBlockingQueueTest, testBlockingConsumer) {
    size_t size = 5;
    UnboundedBlockingQueue<int> queue(size);

    UnboundedProducerWorker producerWorker(queue);
    producerWorker.produce(5);

    UnboundedConsumerWorker consumerWorker(queue);
    consumerWorker.consume(8);

    producerWorker.pushNumbers(3);

    producerWorker.join();
    consumerWorker.join();

    size_t zero = 0;
    ASSERT_EQ(zero, queue.size());
}

TEST(UnboundedBlockingQueueTest, testTimeout) {
    size_t size = 5;
    UnboundedBlockingQueue<int> queue(size);
    int value;
    bool popReturn = queue.pop(value, std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_FALSE(popReturn);
}

TEST(UnboundedBlockingQueueTest, testCloseInterruptOnEmpty) {
    UnboundedBlockingQueue<int> queue(10);
    pulsar::Latch latch(1);

    auto thread = std::thread([&]() {
        int v;
        bool res = queue.pop(v);
        ASSERT_FALSE(res);
        latch.countdown();
    });

    // Sleep to allow for background thread to call pop and be blocked there
    std::this_thread::sleep_for(std::chrono::seconds(1));

    queue.close();
    bool wasUnblocked = latch.wait(std::chrono::seconds(5));

    ASSERT_TRUE(wasUnblocked);
    thread.join();
}
