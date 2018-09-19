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
  "github.com/spf13/cobra"
  "github.com/spf13/pflag"

  "github.com/apache/pulsar/cli/admin"
)

var ClientFlags = ClientMeta{}

func NewDefaultPulsarCommand() *cobra.Command {
  cmds := &cobra.Command{
    Use: "pulsarctl",
  }
  
  cmds.AddCommand(NewClustersCommands())

  cmds.PersistentFlags().AddFlagSet(ClientFlags.FlagSet())

  return cmds
}


type ClientMeta struct {
  // Admin Service address to which to connect. Default: http://localhost:8080/
  Address string
}

func (c *ClientMeta) FlagSet() *pflag.FlagSet {
  flags := pflag.NewFlagSet("", pflag.ContinueOnError)
  flags.StringVar(&c.Address, "address", admin.DefaultServiceURL, "Admin Service address to which to connect")
  return flags
}

func (c *ClientMeta) Client() admin.Client {
  config := admin.DefaultConfig()

  if len(c.Address) > 0 && c.Address != config.Address {
    config.Address = c.Address
  }

  return admin.New(config)
}
