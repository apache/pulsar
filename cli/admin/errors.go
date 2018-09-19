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

package admin

import(
  "fmt"
  "strings"
)

const unknownErrorReason = "Unknown pulsar error"

type Error struct {
  Reason string `json:"reason"`
  Code int
}

func (e Error) Error() string {
  return fmt.Sprintf("code: %d reason: %s", e.Code, e.Reason)
}

func IsAdminError(err error) bool {
  _, ok := err.(Error)
  return ok
}

func IsUnknownError(err error) bool {
  if err == nil {
    return false
  }

  if pe, ok := err.(Error); ok {
    return pe.Code == 500 && strings.Contains(pe.Reason, unknownErrorReason)
  }
  return false
}
