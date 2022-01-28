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

AuthenticationWrapper::AuthenticationWrapper() {}

AuthenticationWrapper::AuthenticationWrapper(const std::string& dynamicLibPath,
                                             const std::string& authParamsString) {
    this->auth = AuthFactory::create(dynamicLibPath, authParamsString);
}

struct AuthenticationTlsWrapper : public AuthenticationWrapper {
    AuthenticationTlsWrapper(const std::string& certificatePath, const std::string& privateKeyPath)
        : AuthenticationWrapper() {
        this->auth = AuthTls::create(certificatePath, privateKeyPath);
    }
};

struct TokenSupplierWrapper {
    PyObject* _pySupplier;

    TokenSupplierWrapper(py::object pySupplier) : _pySupplier(pySupplier.ptr()) { Py_XINCREF(_pySupplier); }

    TokenSupplierWrapper(const TokenSupplierWrapper& other) {
        _pySupplier = other._pySupplier;
        Py_XINCREF(_pySupplier);
    }

    TokenSupplierWrapper& operator=(const TokenSupplierWrapper& other) {
        _pySupplier = other._pySupplier;
        Py_XINCREF(_pySupplier);
        return *this;
    }

    virtual ~TokenSupplierWrapper() { Py_XDECREF(_pySupplier); }

    std::string operator()() {
        PyGILState_STATE state = PyGILState_Ensure();

        std::string token;
        try {
            token = py::call<std::string>(_pySupplier);
        } catch (const py::error_already_set& e) {
            PyErr_Print();
        }

        PyGILState_Release(state);
        return token;
    }
};

struct AuthenticationTokenWrapper : public AuthenticationWrapper {
    AuthenticationTokenWrapper(py::object token) : AuthenticationWrapper() {
        if (py::extract<std::string>(token).check()) {
            // It's a string
            std::string tokenStr = py::extract<std::string>(token);
            this->auth = AuthToken::createWithToken(tokenStr);
        } else {
            // It's a function object
            this->auth = AuthToken::create(TokenSupplierWrapper(token));
        }
    }
};

struct AuthenticationAthenzWrapper : public AuthenticationWrapper {
    AuthenticationAthenzWrapper(const std::string& authParamsString) : AuthenticationWrapper() {
        this->auth = AuthAthenz::create(authParamsString);
    }
};

struct AuthenticationOauth2Wrapper : public AuthenticationWrapper {
    AuthenticationOauth2Wrapper(const std::string& authParamsString) : AuthenticationWrapper() {
        this->auth = AuthOauth2::create(authParamsString);
    }
};

void export_authentication() {
    using namespace boost::python;

    class_<AuthenticationWrapper>("Authentication", init<const std::string&, const std::string&>());

    class_<AuthenticationTlsWrapper, bases<AuthenticationWrapper> >(
        "AuthenticationTLS", init<const std::string&, const std::string&>());

    class_<AuthenticationTokenWrapper, bases<AuthenticationWrapper> >("AuthenticationToken",
                                                                      init<py::object>());

    class_<AuthenticationAthenzWrapper, bases<AuthenticationWrapper> >("AuthenticationAthenz",
                                                                       init<const std::string&>());

    class_<AuthenticationOauth2Wrapper, bases<AuthenticationWrapper> >("AuthenticationOauth2",
                                                                       init<const std::string&>());
}
