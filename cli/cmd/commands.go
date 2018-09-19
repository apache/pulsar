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
