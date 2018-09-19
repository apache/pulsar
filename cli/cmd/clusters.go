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

  "github.com/apache/pulsar/cli/admin"
)

// NewClustersCommands
func NewClustersCommands() *cobra.Command {
  cmds := &cobra.Command{
    Use: "clusters",
    Aliases: []string{"cluster"},
  }

  cmds.AddCommand(NewListClustersCommand())
  cmds.AddCommand(NewGetClustersCommand())
  cmds.AddCommand(NewRegisterClusterCommand())
  cmds.AddCommand(NewRemoveClusterCommand())

  return cmds
}

func NewListClustersCommand() *cobra.Command {
  c := &cobra.Command{
    Use: "list",
    Short: "List the existing clusters",
    Long: "List the existing clusters",
    Run: func(cmd *cobra.Command, args []string) {
      client := NewClient()
      clusters, err := client.Clusters().List()
      if err != nil {
        PrintError(cmd.OutOrStderr(), err)
        return
      }
      for _, c := range clusters {
        cmd.Println(c)
      }
    },
  }

  return c
}

func NewGetClustersCommand() *cobra.Command {
  c := &cobra.Command{
    Use: "get NAME",
    Short: "Get the configuration data for the specified cluster",
    Long: "Get the configuration data for the specified cluster",
    Args: cobra.ExactArgs(1),
    Run: func(cmd *cobra.Command, args []string) {
      client := NewClient()
      cluster, err := client.Clusters().Get(args[0])
      if err != nil {
        PrintError(cmd.OutOrStderr(), err)
        return
      }
      PrintJson(cmd.OutOrStdout(), cluster)
    },
  }

  return c
}


var createClusterExample = `
  # Create a cluster with a service url
  pulsarctl clusters register cluster-us-west --url=http://us-west.org.com:8080

  # Create a cluster with service url and broker url
  pulsarctl clusters register cluster-us-west --url=http://us-west.org.com:8080 --broker-url=http://us-west.org.com:6650
`

func NewRegisterClusterCommand() *cobra.Command {
  
  c := &cobra.Command{
    Use: "register NAME",
    Short: "Register configuration data for a cluster. This operation requires Pulsar super-user privileges",
    Long: "Register configuration data for a cluster. This operation requires Pulsar super-user privileges",
    Example: createClusterExample,
    Args: cobra.ExactArgs(1),
    Run: func(cmd *cobra.Command, args []string) {
      name := args[0]
      cluster := admin.ClusterData{
        Name: name,
        ServiceURL: admin.DefaultServiceURL,
        BrokerServiceURL: admin.DefaultBrokerURL,
      }

      client := NewClient()
      if err := client.Clusters().Create(cluster); err != nil {
        PrintError(cmd.OutOrStdout(), err)
        return
      }
      cmd.Printf("cluster %s registered\n", name)
    },
  }

  flags := c.Flags()
  flags.String("url", admin.DefaultServiceURL, "url for the accessing the admin rest api")
  flags.String("broker-url", admin.DefaultBrokerURL, "url for accessing the binary api")

  return c
}

func NewRemoveClusterCommand() *cobra.Command {
  
  c := &cobra.Command{
    Use: "remove NAME",
    Short: "Removes the configuration data for the specified cluster",
    Long: "Removes the configuration data for the specified cluster",
    Example: createClusterExample,
    Args: cobra.ExactArgs(1),
    Run: func(cmd *cobra.Command, args []string) {
      name := args[0]
      client := NewClient()
      if err := client.Clusters().Delete(name); err != nil {
        PrintError(cmd.OutOrStderr(), err)
        return
      }

      cmd.Printf("configuration for cluster %s has been removed.\n", name)
    },
  }

  return c
}

