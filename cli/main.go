package main

import(
  "fmt"
  "os"

  "github.com/apache/pulsar/cli/cmd"
)

func run() error {
  cmd := cmd.NewDefaultPulsarCommand()
  return cmd.Execute()
}

func main() {
  if err := run(); err != nil {
    fmt.Fprintf(os.Stderr, "error: %v\n", err)
    os.Exit(1)
  }
  os.Exit(0)
}
