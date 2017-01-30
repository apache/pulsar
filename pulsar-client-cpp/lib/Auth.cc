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
#include <stdio.h>

#include <pulsar/Auth.h>
#include <lib/LogUtils.h>

#include <string>
#include <vector>
#include <iostream>
#include <dlfcn.h>
#include <cstdlib>
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

// Built-in authentications
#include "auth/AuthTls.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

Authentication::Authentication() {
}

Authentication::~Authentication() {
}

class AuthDisabled : public Authentication {
 public:
    AuthDisabled()
            : Authentication() {
    }

    static AuthenticationPtr create() {
        return boost::make_shared<AuthDisabled>();
    }

    const std::string getAuthMethodName() const {
        return "none";
    }

    Result getAuthData(std::string&) const {
        return ResultOk;
    }
};

AuthenticationPtr Auth::Disabled() {
    return AuthDisabled::create();
}

AuthenticationPtr Auth::create(const std::string& dynamicLibPath) {
    return Auth::create(dynamicLibPath, "");
}

boost::mutex mutex;
std::vector<void*> Auth::loadedLibrariesHandles_;
bool Auth::isShutdownHookRegistered_ = false;

void Auth::release_handles() {
    boost::lock_guard<boost::mutex> lock(mutex);
    for (std::vector<void*>::iterator ite = Auth::loadedLibrariesHandles_.begin(); ite != Auth::loadedLibrariesHandles_.end();
            ite++) {
        dlclose(*ite);
    }
    loadedLibrariesHandles_.clear();
}

AuthenticationPtr Auth::create(const std::string& dynamicLibPath, const std::string& params) {
    {
        boost::lock_guard<boost::mutex> lock(mutex);
        if (!Auth::isShutdownHookRegistered_) {
            atexit(release_handles);
            Auth::isShutdownHookRegistered_ = true;
        }
    }
    Authentication *auth = NULL;
    void *handle = dlopen(dynamicLibPath.c_str(), RTLD_LAZY);
    if (handle != NULL) {
        {
            boost::lock_guard<boost::mutex> lock(mutex);
            loadedLibrariesHandles_.push_back(handle);
        }
        Authentication *(*createAuthentication)(const std::string);
        *(void **) (&createAuthentication) = dlsym(handle, "create");
        if (createAuthentication != NULL) {
            auth = createAuthentication(params);
        }
    }
    return boost::shared_ptr<Authentication>(auth);
}

}
