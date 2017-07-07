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

#include "utils.h"

void Producer_send(Producer& producer, const Message& message) {
    CHECK_RESULT(producer.send(message));
}

void Producer_sendAsyncCallback(PyObject* callback, Result res, const Message& msg) {
    if (callback == Py_None) {
        return;
    }

    PyGILState_STATE state = PyGILState_Ensure();

    try {
        py::call<void>(callback, res, py::object(&msg));
    } catch (py::error_already_set e) {
        PyErr_Print();
    }

    Py_XDECREF(callback);
    PyGILState_Release(state);
}

void Producer_sendAsync(Producer& producer, const Message& message, py::object callback) {
    PyObject* pyCallback = callback.ptr();
    Py_XINCREF(pyCallback);

    Py_BEGIN_ALLOW_THREADS
    producer.sendAsync(message, boost::bind(Producer_sendAsyncCallback, pyCallback, _1, _2));
    Py_END_ALLOW_THREADS
}

void Producer_close(Producer& producer) {
    CHECK_RESULT(producer.close());
}

void export_producer() {
    using namespace boost::python;

    class_<Producer>("Producer", no_init)
            .def("topic", &Producer::getTopic, "return the topic to which producer is publishing to",
                 return_value_policy<copy_const_reference>())
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
            .def("close", &Producer_close)
            ;

}