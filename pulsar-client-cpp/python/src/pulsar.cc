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

void export_client();
void export_message();
void export_producer();
void export_consumer();
void export_config();
void export_enums();
void export_authentication();


inline void translateException(const PulsarException& ex) {
    std::string err = "Pulsar error: ";
    err += strResult(ex._result);
    PyErr_SetString(PyExc_Exception, err.c_str());
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
    export_config();
    export_enums();
    export_authentication();
}
