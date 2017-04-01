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
#ifndef PULSAR_CPP_CLIENTCONNECTIONCONTAINER_H
#define PULSAR_CPP_CLIENTCONNECTIONCONTAINER_H

#include <vector>
#include <iostream>
#include <algorithm>    // std::min
namespace pulsar {
/* @brief - This class uses a vector to store elements and provides a wrap around getNext() function to retrieve elements in a round robin fashion.
 * @note - this class is not thread safe.
 */
template<class T>
class ClientConnectionContainer {
 private:
    size_t capacity_;
    size_t currentIndex_;
    std::vector<T> list_;
 public:
    /*
     * @param - the capacity of the container (capacity > 0)
     * @note - A container of capacity 1 is created if given capacity is 0.
     */
    ClientConnectionContainer(size_t);

    /*
     * @returns - true if the container has reached it's max capacity.
     */
    inline bool isFull() const;

    /*
     * @brief - gets the next element in the container - wraps around after the last element.
     * @returns - false if the list is empty.
     */
    bool getNext(T&);

    /*
     * @brief - Adds the element to the end of the list.
     * @returns - false if the list is full.
     */
    bool add(T&);

    /*
     * @brief - removes element in reverse order starting from the one returned by last getNext() call
     * 		  - removes the oldest element if getNext() never called
     * @return - true if an element was removed
     */
    bool remove();

    /*
     * @returns - the size of the container
     */
    inline size_t size() const;

    /*
     * @returns - the capacity of the container
     */
    inline size_t capacity() const;

    /*
     * @returns - true if the list is empty
     */
    inline bool isEmpty() const;

    // http://web.mst.edu/~nmjxv3/articles/templates.html
    friend std::ostream& operator<<(std::ostream& os, const ClientConnectionContainer<T>& obj) {
        os << "ClientConnectionContainer [ size_ = " << obj.size() << ", currentIndex_ = "
                << obj.currentIndex_ << ", capacity = " << obj.capacity_ << "]";
        return os;
    }
};

template<class T> ClientConnectionContainer<T>::ClientConnectionContainer(size_t capacity)
        : capacity_(std::max(capacity, 1uL)),
          currentIndex_(-1) {
}

template<class T> bool ClientConnectionContainer<T>::isFull() const {
    return list_.size() >= capacity_;
}

template<class T> bool ClientConnectionContainer<T>::isEmpty() const {
    return list_.size() == 0;
}

template<class T> bool ClientConnectionContainer<T>::getNext(T& element) {
    if (list_.empty()) {
        return false;
    }
    currentIndex_ = (currentIndex_ + 1) % list_.size();
    element = list_[currentIndex_];
    return true;
}

template<class T> bool ClientConnectionContainer<T>::add(T& element) {
    if (isFull()) {
        return false;
    }
    list_.push_back(element);
    return true;
}

template<class T> bool ClientConnectionContainer<T>::remove() {
    if (list_.empty()) {
        return false;
    } else if (list_.size() == 1) {
        list_.clear();
        currentIndex_ = -1;
        return true;
    } else if (currentIndex_ == -1) {
        list_.erase(list_.begin());
        return true;
    }
    // list size >= 2
    list_.erase(list_.begin() + currentIndex_);
    // list size >= 1
    currentIndex_ = (list_.size() + currentIndex_ - 1) % list_.size();
    return true;
}

template<class T> size_t ClientConnectionContainer<T>::size() const {
    return list_.size();
}

template<class T> size_t ClientConnectionContainer<T>::capacity() const {
    return capacity_;
}
}

#endif //PULSAR_CPP_CLIENTCONNECTIONCONTAINER_H
