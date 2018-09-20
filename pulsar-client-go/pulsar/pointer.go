//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

// #include <stdlib.h>
import "C"
import (
	"unsafe"
	"sync"
)

// Inspired by https://github.com/mattn/go-pointer
// Make sure the marker pointer is freed after restoring

var (
	mutex sync.Mutex
	pointers = map[unsafe.Pointer]interface{}{}
)

func savePointer(object interface{}) unsafe.Pointer {
	// Get a ref to object using reflection
	ptr := C.malloc(C.size_t(1))

	mutex.Lock()
	pointers[ptr] = object
	mutex.Unlock()

	return ptr
}

func restorePointer(ptr unsafe.Pointer) interface{} {
	mutex.Lock()
	obj := pointers[ptr]
	delete(pointers, ptr)
	C.free(ptr)
	mutex.Unlock()

	return obj
}

func restorePointerNoDelete(ptr unsafe.Pointer) interface{} {
	mutex.Lock()
	obj := pointers[ptr]
	mutex.Unlock()

	return obj
}
