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

#pragma once

#include <boost/python.hpp>

#include <pulsar/Client.h>
#include <pulsar/MessageBatch.h>
#include <lib/Utils.h>

using namespace pulsar;

namespace py = boost::python;

struct PulsarException {
    Result _result;
    PulsarException(Result res) : _result(res) {}
};

inline void CHECK_RESULT(Result res) {
    if (res != ResultOk) {
        throw PulsarException(res);
    }
}

void waitForAsyncResult(std::function<void(ResultCallback)> func);

template <typename T, typename Callback>
inline void waitForAsyncValue(std::function<void(Callback)> func, T& value) {
    Result res = ResultOk;
    Promise<Result, T> promise;
    Future<Result, T> future = promise.getFuture();

    Py_BEGIN_ALLOW_THREADS func(WaitForCallbackValue<T>(promise));
    Py_END_ALLOW_THREADS

        bool isComplete;
    while (true) {
        // Check periodically for Python signals
        Py_BEGIN_ALLOW_THREADS isComplete = future.get(res, std::ref(value), std::chrono::milliseconds(100));
        Py_END_ALLOW_THREADS

            if (isComplete) {
            CHECK_RESULT(res);
            return;
        }

        if (PyErr_CheckSignals() == -1) {
            PyErr_SetInterrupt();
            return;
        }
    }
}

struct AuthenticationWrapper {
    AuthenticationPtr auth;

    AuthenticationWrapper();
    AuthenticationWrapper(const std::string& dynamicLibPath, const std::string& authParamsString);
};

struct CryptoKeyReaderWrapper {
    CryptoKeyReaderPtr cryptoKeyReader;

    CryptoKeyReaderWrapper();
    CryptoKeyReaderWrapper(const std::string& publicKeyPath, const std::string& privateKeyPath);
};
