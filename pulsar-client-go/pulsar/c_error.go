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

import "C"

/*
#include "c_go_pulsar.h"
*/
import "C"
import "fmt"

func cBool(flag bool) C.int {
	if flag {
		return C.int(1)
	} else {
		return C.int(0)
	}
}

type Error struct {
	msg    string
	result Result
}

func newError(result C.pulsar_result, msg string) error {
	return &Error{
		msg:    fmt.Sprintf("%s: %v", msg, C.GoString(C.pulsar_result_str(result))),
		result: Result(result),
	}
}

func (e *Error) Result() Result {
	return e.result
}

func (e *Error) Error() string {
	return e.msg
}

func (r Result) String() string {
	return C.GoString(C.pulsar_result_str(C.pulsar_result(r)))
}
