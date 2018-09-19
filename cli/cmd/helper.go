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
    fmt.Fprintf(w, "expected response type: %v\n", err)
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
