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

#include "utils.h"

void waitForAsyncResult(std::function<void(ResultCallback)> func) {
    Result res = ResultOk;
    bool b;
    Promise<bool, Result> promise;
    Future<bool, Result> future = promise.getFuture();

    Py_BEGIN_ALLOW_THREADS func(WaitForCallback(promise));
    Py_END_ALLOW_THREADS

        bool isComplete;
    while (true) {
        // Check periodically for Python signals
        Py_BEGIN_ALLOW_THREADS isComplete = future.get(b, std::ref(res), std::chrono::milliseconds(100));
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
