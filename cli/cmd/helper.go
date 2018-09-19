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

package cmd

import(
  "encoding/json"
  "fmt"
  "io"

  "github.com/apache/pulsar/cli/admin"
)

func NewClient() admin.Client {
  return ClientFlags.Client()
}


func PrintJson(w io.Writer, obj interface{}) {
  b, err := json.MarshalIndent(obj, "", "  ")
  if err != nil {
    fmt.Fprintf(w, "unexpected response type: %v\n", err)
    return
  }
  fmt.Fprintln(w, string(b))
}


func PrintError(w io.Writer, err error) {
  msg := err.Error()
  if admin.IsAdminError(err) {
    ae, _ := err.(admin.Error)
    msg = ae.Reason
  }
  fmt.Fprintln(w, "error:", msg)
}
