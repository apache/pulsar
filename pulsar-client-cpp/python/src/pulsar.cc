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

void export_client();
void export_message();
void export_producer();
void export_consumer();
void export_reader();
void export_config();
void export_enums();
void export_authentication();
void export_schema();
void export_cryptoKeyReader();
void export_exceptions();

PyObject* get_exception_class(Result result);


static void translateException(const PulsarException& ex) {
    std::string err = "Pulsar error: ";
    err += strResult(ex._result);

    PyErr_SetString(get_exception_class(ex._result), err.c_str());
}

BOOST_PYTHON_MODULE(_pulsar)
{
    py::register_exception_translator<PulsarException>(translateException);

    // Initialize thread support so that we can grab the GIL mutex
    // from pulsar library threads
    PyEval_InitThreads();

    export_client();
    export_message();
    export_producer();
    export_consumer();
    export_reader();
    export_config();
    export_enums();
    export_authentication();
    export_schema();
    export_cryptoKeyReader();
    export_exceptions();
}
