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

Message Reader_readNext(Reader& reader) {
    Message msg;
    Result res;

    while (true) {
        Py_BEGIN_ALLOW_THREADS
        // Use 100ms timeout to periodically check whether the
        // interpreter was interrupted
        res = reader.readNext(msg, 100);
        Py_END_ALLOW_THREADS

        if (res != ResultTimeout) {
            // In case of timeout we keep calling receive() to simulate a
            // blocking call until a message is available, while breaking
            // every once in a while to check the Python signal status
            break;
        }

        if (PyErr_CheckSignals() == -1) {
            PyErr_SetInterrupt();
            return msg;
        }
    }

    CHECK_RESULT(res);
    return msg;
}

Message Reader_readNextTimeout(Reader& reader, int timeoutMs) {
    Message msg;
    Result res;
    Py_BEGIN_ALLOW_THREADS
    res = reader.readNext(msg, timeoutMs);
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
    return msg;
}

bool Reader_hasMessageAvailable(Reader& reader) {
    bool available = false;
    Result res;
    Py_BEGIN_ALLOW_THREADS
        res = reader.hasMessageAvailable(available);
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
    return available;
}

void Reader_close(Reader& reader) {
    Result res;
    Py_BEGIN_ALLOW_THREADS
    res = reader.close();
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
}

void export_reader() {
    using namespace boost::python;

    class_<Reader>("Reader", no_init)
            .def("topic", &Reader::getTopic, return_value_policy<copy_const_reference>())
            .def("read_next", &Reader_readNext)
            .def("read_next", &Reader_readNextTimeout)
            .def("has_message_available", &Reader_hasMessageAvailable)
            .def("close", &Reader_close)
            ;
}
