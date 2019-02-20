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

package main

import (
	"github.com/spf13/cobra"
)

type ClientArgs struct {
	ServiceUrl      string
	IoThreads       int
	ListenerThreads int
}

var clientArgs ClientArgs

func main() {
	initProducer()
	initConsumer()

	var rootCmd = &cobra.Command{Use: "pulsar-perf-go"}
	rootCmd.Flags().StringVarP(&clientArgs.ServiceUrl, "service-url", "u",
		"pulsar://localhost:6650", "The Pulsar service URL")
	rootCmd.Flags().IntVar(&clientArgs.IoThreads, "io-threads",
		1, "The number of IO threads in client")
	rootCmd.Flags().IntVar(&clientArgs.ListenerThreads, "listener-threads",
		1, "The number of listener threads in client")
	rootCmd.AddCommand(cmdProduce, cmdConsume)

	rootCmd.Execute()
}
