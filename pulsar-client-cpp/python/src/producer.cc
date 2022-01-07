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

#include <functional>

extern boost::python::object MessageId_serialize(const MessageId& msgId);

boost::python::object Producer_send(Producer& producer, const Message& message) {
    Result res;
    MessageId messageId;
    Py_BEGIN_ALLOW_THREADS res = producer.send(message, messageId);
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
    return MessageId_serialize(messageId);
}

void Producer_sendAsyncCallback(PyObject* callback, Result res, const MessageId& msgId) {
    if (callback == Py_None) {
        return;
    }

    PyGILState_STATE state = PyGILState_Ensure();

    try {
        py::call<void>(callback, res, py::object(&msgId));
    } catch (const py::error_already_set& e) {
        PyErr_Print();
    }

    Py_XDECREF(callback);
    PyGILState_Release(state);
}

void Producer_sendAsync(Producer& producer, const Message& message, py::object callback) {
    PyObject* pyCallback = callback.ptr();
    Py_XINCREF(pyCallback);

    Py_BEGIN_ALLOW_THREADS producer.sendAsync(
        message,
        std::bind(Producer_sendAsyncCallback, pyCallback, std::placeholders::_1, std::placeholders::_2));
    Py_END_ALLOW_THREADS
}

void Producer_flush(Producer& producer) {
    Result res;
    Py_BEGIN_ALLOW_THREADS res = producer.flush();
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
}

void Producer_close(Producer& producer) {
    Result res;
    Py_BEGIN_ALLOW_THREADS res = producer.close();
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
}

bool Producer_is_connected(Producer& producer) { return producer.isConnected(); }

void export_producer() {
    using namespace boost::python;

    class_<Producer>("Producer", no_init)
        .def("topic", &Producer::getTopic, "return the topic to which producer is publishing to",
             return_value_policy<copy_const_reference>())
        .def("producer_name", &Producer::getProducerName,
             "return the producer name which could have been assigned by the system or specified by the "
             "client",
             return_value_policy<copy_const_reference>())
        .def("last_sequence_id", &Producer::getLastSequenceId)
        .def("send", &Producer_send,
             "Publish a message on the topic associated with this Producer.\n"
             "\n"
             "This method will block until the message will be accepted and persisted\n"
             "by the broker. In case of errors, the client library will try to\n"
             "automatically recover and use a different broker.\n"
             "\n"
             "If it wasn't possible to successfully publish the message within the sendTimeout,\n"
             "an error will be returned.\n"
             "\n"
             "This method is equivalent to asyncSend() and wait until the callback is triggered.\n"
             "\n"
             "@param msg message to publish\n")
        .def("send_async", &Producer_sendAsync)
        .def("flush", &Producer_flush,
             "Flush all the messages buffered in the client and wait until all messages have been\n"
             "successfully persisted\n")
        .def("close", &Producer_close)
        .def("is_connected", &Producer_is_connected);
}
