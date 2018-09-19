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
