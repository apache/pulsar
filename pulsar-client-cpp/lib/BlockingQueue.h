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
#ifndef LIB_BLOCKINGQUEUE_H_
#define LIB_BLOCKINGQUEUE_H_

#include <assert.h>
#include <mutex>
#include <condition_variable>
#include <boost/circular_buffer.hpp>

/**
 * Following structs are defined for holding a predicate in wait() call on condition variables.
 * This is done in order to avoid spurious wake up problem.
 * Details: https://www.justsoftwaresolutions.co.uk/threading/condition-variable-spurious-wakes.html
 */
template <typename Container>
struct QueueNotEmpty {
    const Container& queue_;
    QueueNotEmpty(const Container& queue) : queue_(queue) {}
    bool operator()() const { return !queue_.isEmptyNoMutex(); }
};

template <typename Container>
struct QueueNotFull {
    const Container& queue_;
    QueueNotFull(const Container& queue) : queue_(queue) {}
    bool operator()() const { return !queue_.isFullNoMutex(); }
};

template <typename T>
class BlockingQueue {
   public:
    typedef typename boost::circular_buffer<T> Container;
    typedef typename Container::iterator iterator;
    typedef typename Container::const_iterator const_iterator;

    class ReservedSpot {
       public:
        ReservedSpot() : queue_(), released_(true) {}

        ReservedSpot(BlockingQueue<T>& queue) : queue_(&queue), released_(false) {}

        ~ReservedSpot() { release(); }

        void release() {
            if (!released_) {
                queue_->releaseReservedSpot();
                released_ = true;
            }
        }

       private:
        BlockingQueue<T>* queue_;
        bool released_;

        friend class BlockingQueue<T>;
    };

    BlockingQueue(size_t maxSize) : maxSize_(maxSize), mutex_(), queue_(maxSize), reservedSpots_(0) {}

    bool tryReserve(size_t noOfSpots) {
        assert(noOfSpots <= maxSize_);
        Lock lock(mutex_);
        if (noOfSpots <= maxSize_ - (reservedSpots_ + queue_.size())) {
            reservedSpots_ += noOfSpots;
            return true;
        }
        return false;
    }

    void reserve(size_t noOfSpots) {
        assert(noOfSpots <= maxSize_);
        Lock lock(mutex_);
        while (noOfSpots--) {
            queueFullCondition.wait(lock, QueueNotFull<BlockingQueue<T> >(*this));
            reservedSpots_++;
        }
    }

    void release(size_t noOfSpots) {
        Lock lock(mutex_);
        assert(noOfSpots <= reservedSpots_);
        bool wasFull = isFullNoMutex();
        reservedSpots_ -= noOfSpots;
        lock.unlock();

        if (wasFull) {
            // Notify that one spot is now available
            queueFullCondition.notify_all();
        }
    }

    ReservedSpot reserve() {
        Lock lock(mutex_);
        // If the queue is full, wait for space to be available
        queueFullCondition.wait(lock, QueueNotFull<BlockingQueue<T> >(*this));
        reservedSpots_++;
        return ReservedSpot(*this);
    }

    void push(const T& value, bool wasReserved = false) {
        Lock lock(mutex_);
        if (wasReserved) {
            reservedSpots_--;
        }
        // If the queue is full, wait for space to be available
        queueFullCondition.wait(lock, QueueNotFull<BlockingQueue<T> >(*this));
        bool wasEmpty = queue_.empty();
        queue_.push_back(value);
        lock.unlock();
        if (wasEmpty) {
            // Notify that an element is pushed
            queueEmptyCondition.notify_all();
        }
    }

    void push(const T& value, ReservedSpot& spot) {
        Lock lock(mutex_);

        // Since the value already had a spot reserved in the queue, we need to
        // discount it
        assert(reservedSpots_ > 0);
        reservedSpots_--;
        spot.released_ = true;

        bool wasEmpty = queue_.empty();
        queue_.push_back(value);
        lock.unlock();

        if (wasEmpty) {
            // Notify that an element is pushed
            queueEmptyCondition.notify_all();
        }
    }

    bool tryPush(const T& value) {
        Lock lock(mutex_);

        // Need to consider queue_.size() + reserved spot
        if (isFullNoMutex()) {
            return false;
        }

        bool wasEmpty = queue_.empty();
        queue_.push_back(value);
        lock.unlock();

        if (wasEmpty) {
            // Notify that an element is pushed
            queueEmptyCondition.notify_all();
        }

        return true;
    }

    void pop() {
        Lock lock(mutex_);
        // If the queue is empty, wait until an element is available to be popped
        queueEmptyCondition.wait(lock, QueueNotEmpty<BlockingQueue<T> >(*this));

        bool wasFull = isFullNoMutex();
        queue_.pop_front();
        lock.unlock();

        if (wasFull) {
            // Notify that an element is popped
            queueFullCondition.notify_all();
        }
    }

    void pop(T& value) {
        Lock lock(mutex_);
        // If the queue is empty, wait until an element is available to be popped
        queueEmptyCondition.wait(lock, QueueNotEmpty<BlockingQueue<T> >(*this));
        value = queue_.front();

        bool wasFull = isFullNoMutex();
        queue_.pop_front();
        lock.unlock();

        if (wasFull) {
            // Notify that an element is popped
            queueFullCondition.notify_all();
        }
    }

    template <typename Duration>
    bool pop(T& value, const Duration& timeout) {
        Lock lock(mutex_);
        if (!queueEmptyCondition.wait_for(lock, timeout, QueueNotEmpty<BlockingQueue<T> >(*this))) {
            return false;
        }

        bool wasFull = isFullNoMutex();
        value = queue_.front();
        queue_.pop_front();
        lock.unlock();

        if (wasFull) {
            // Notify that an element is popped
            queueFullCondition.notify_all();
        }

        return true;
    }

    // Check the 1st element of the queue
    bool peek(T& value) {
        Lock lock(mutex_);
        if (queue_.empty()) {
            return false;
        }

        value = queue_.front();
        return true;
    }

    // Remove all elements from the queue
    void clear() {
        Lock lock(mutex_);
        queue_.clear();
        queueFullCondition.notify_all();
    }

    size_t size() const {
        Lock lock(mutex_);
        return queue_.size();
    }

    size_t maxSize() const { return maxSize_; }

    bool empty() const {
        Lock lock(mutex_);
        return isEmptyNoMutex();
    }

    bool full() const {
        Lock lock(mutex_);
        return isFullNoMutex();
    }

    const_iterator begin() const { return queue_.begin(); }

    const_iterator end() const { return queue_.end(); }

    iterator begin() { return queue_.begin(); }

    iterator end() { return queue_.end(); }

    int reservedSpots() const { return reservedSpots_; }

   private:
    void releaseReservedSpot() {
        Lock lock(mutex_);
        bool wasFull = isFullNoMutex();
        --reservedSpots_;
        lock.unlock();

        if (wasFull) {
            // Notify that one spot is now available
            queueFullCondition.notify_all();
        }
    }

    bool isEmptyNoMutex() const { return queue_.empty(); }

    bool isFullNoMutex() const { return (queue_.size() + reservedSpots_) == maxSize_; }

    const size_t maxSize_;
    mutable std::mutex mutex_;
    std::condition_variable queueFullCondition;
    std::condition_variable queueEmptyCondition;
    Container queue_;
    int reservedSpots_;

    typedef std::unique_lock<std::mutex> Lock;
    friend class QueueReservedSpot;
    friend struct QueueNotEmpty<BlockingQueue<T> >;
    friend struct QueueNotFull<BlockingQueue<T> >;
};

#endif /* LIB_BLOCKINGQUEUE_H_ */
