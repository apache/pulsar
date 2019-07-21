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
#ifndef LIB_OBJECTPOOL_H_
#define LIB_OBJECTPOOL_H_

#include <algorithm>
#include <mutex>
#include <memory>

namespace pulsar {

template <typename T, int MaxSize>
class Allocator {
   public:
    // Allocator must be stateless, so put everything in this static
    class Impl {
       public:
        // cheap lock to acquire
        static std::mutex mutex_;

        // note: use std::forward_list<> when switching to C++11 mode
        struct Node {
            Node* next;
            explicit Node(Node* n) : next(n) {}
        };
        Node* head_;
        int pushSize_;

        struct GlobalPool {
            Node* node_;
            int nodeCount_;
            GlobalPool* next_;
            explicit GlobalPool(GlobalPool* n) : next_(n) {}
        };
        static struct GlobalPool* globalPool_;
        static int globalNodeCount_;

        Impl(const Impl&);
        void operator=(const Impl&);

        void* pop() {
            if (!head_) {
                // size = 0
                std::lock_guard<std::mutex> lock(mutex_);

                if (!globalPool_) {
                    return NULL;
                }

                GlobalPool* poolEntry = globalPool_;
                head_ = globalPool_->node_;
                pushSize_ += globalPool_->nodeCount_;
                globalNodeCount_ -= globalPool_->nodeCount_;
                globalPool_ = globalPool_->next_;
                delete poolEntry;
            }
            void* result = head_;
            if (result) {
                head_ = head_->next;
                pushSize_--;
            }
            return result;
        }

        bool push(void* p) {
            // Once thread specific entries reaches 10% of max size, push them to GlobalPool
            if (pushSize_ >= MaxSize * 0.1) {
                bool deleteList = true;
                {
                    // Move the entries to global pool
                    std::lock_guard<std::mutex> lock(mutex_);

                    // If total node count reached max allowed cache limit,
                    // skip adding to global pool.
                    if ((globalNodeCount_ + pushSize_) <= MaxSize) {
                        deleteList = false;

                        globalPool_ = new GlobalPool(globalPool_);
                        globalPool_->node_ = head_;
                        globalPool_->nodeCount_ = pushSize_;
                        globalNodeCount_ += pushSize_;
                    }
                }
                if (deleteList) {
                    pushSize_ = 0;
                    deleteLinkedList(head_);
                }
                head_ = new (p) Node(0);
                pushSize_ = 1;
                return true;
            }

            head_ = new (p) Node(head_);
            pushSize_++;
            return true;
        }

        static void deleteLinkedList(Node* head) {
            Node* n = head;
            while (n) {
                void* p = n;
                n = n->next;
                ::operator delete(p);
            }
        }

       public:
        Impl() {
            pushSize_ = 0;
            head_ = 0;
        }

        ~Impl() {
            // No need for mutex for pop
            deleteLinkedList(head_);
        }

        void* allocate() {
            void* result = pop();
            if (!result) {
                result = ::operator new(std::max(sizeof(T), sizeof(Node)));
            }
            return result;
        }

        void deallocate(void* p) {
            if (!push(p)) {
                ::operator delete(p);
            }
        }
    };

    static thread_local std::unique_ptr<Impl> implPtr_;
    typedef T value_type;
    typedef size_t size_type;
    typedef T* pointer;
    typedef const void* const_pointer;

    Allocator() {}

    Allocator(const Allocator& /*other*/) {}

    template <typename Other, int OtherSize>
    Allocator(const Allocator<Other, OtherSize>& /*other*/) {}

    pointer allocate(size_type n, const void* /*hint*/ = 0) {
        Impl* impl = implPtr_.get();
        if (!impl) {
            implPtr_.reset(new Impl);
            impl = implPtr_.get();
        }
        void* p = (n == 1) ? impl->allocate() : operator new(n * sizeof(T));
        return static_cast<T*>(p);
    }

    void deallocate(pointer ptr, size_type n) {
        Impl* impl = implPtr_.get();
        if (!impl) {
            implPtr_.reset(new Impl);
            impl = implPtr_.get();
        }
        if (n == 1)
            impl->deallocate(ptr);
        else
            ::operator delete(ptr);
    }

    template <typename Other>
    struct rebind {
        typedef Allocator<Other, MaxSize> other;
    };
};

// typename Allocator<Type,MaxSize>::Impl is important else the compiler
// doesn't understand that it is a type
template <typename Type, int MaxSize>
thread_local std::unique_ptr<typename Allocator<Type, MaxSize>::Impl> Allocator<Type, MaxSize>::implPtr_;

template <typename Type, int MaxSize>
std::mutex Allocator<Type, MaxSize>::Impl::mutex_;

template <typename Type, int MaxSize>
typename Allocator<Type, MaxSize>::Impl::GlobalPool* Allocator<Type, MaxSize>::Impl::globalPool_;

template <typename Type, int MaxSize>
int Allocator<Type, MaxSize>::Impl::globalNodeCount_;

template <typename Type, int MaxSize>
class ObjectPool {
    typedef std::shared_ptr<Type> TypeSharedPtr;

    Allocator<Type, MaxSize> allocator_;

   public:
    ObjectPool() {}

    TypeSharedPtr create() { return std::allocate_shared<Type>(allocator_); }

    ~ObjectPool() {
        struct Allocator<Type, MaxSize>::Impl::GlobalPool* poolEntry =
            Allocator<Type, MaxSize>::Impl::globalPool_;
        while (poolEntry) {
            Allocator<Type, MaxSize>::Impl::deleteLinkedList(poolEntry->node_);
            struct Allocator<Type, MaxSize>::Impl::GlobalPool* delEntry = poolEntry;
            poolEntry = poolEntry->next_;
            ::operator delete(delEntry);
        }
    }

   private:
    ObjectPool<Type, MaxSize>(const ObjectPool<Type, MaxSize>&);
    ObjectPool<Type, MaxSize>& operator=(const ObjectPool<Type, MaxSize>&);
};
}  // namespace pulsar
#endif /* LIB_OBJECTPOOL_H_ */
