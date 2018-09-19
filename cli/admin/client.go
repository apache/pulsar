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

import (
  "bytes"
  "encoding/json"
  "fmt"
  "io"
  "io/ioutil"
  "net/http"
  "net/url"
  "path"
)

const(
  DefaultServiceURL = "http://localhost:8080"
  DefaultBrokerURL = "http://localhost:6650"
)

// Config is used to configure the admin client
type Config struct {
  Address string

  httpClient *http.Client
}

// DefaultConfig returns a default configuration for the pulsar admin client
func DefaultConfig() *Config {
  config := &Config{
    Address: DefaultServiceURL,
    httpClient: http.DefaultClient,
  }
  return config
}

// Client provides a client to the Pulsar Admin API
type Client interface {
  Clusters() Clusters
}

// New returns a new client
func New(config *Config) Client {
  defConfig := DefaultConfig()

  if len(config.Address) == 0 {
    config.Address = defConfig.Address
  }

  c := &client{
    address: config.Address,
  }

  return c
}

type client struct {
  address string

  // TODO allow this to be configurable
  httpClient *http.Client
}

// get is used to do a GET request against an endpoint
// and deserialize the response into an interface
func (c *client) get(endpoint string, obj interface{}) error {
  req, err := c.newRequest(http.MethodGet, endpoint)
  if err != nil {
    return err
  }
  
  resp, err := checkSuccessful(c.doRequest(req))
  if err != nil {
    return err
  }
  defer safeRespClose(resp)

  if obj != nil {
    if err := decodeJsonBody(resp, &obj); err != nil {
      return err
    }
  }

  return nil
}

func (c *client) put(endpoint string, in, obj interface{}) error {
  req, err := c.newRequest(http.MethodPut, endpoint)
  if err != nil {
    return err
  }
  req.obj = in

  resp, err := checkSuccessful(c.doRequest(req))
  if err != nil {
    return err
  }
  defer safeRespClose(resp)
  
  if obj != nil {
    if err := decodeJsonBody(resp, &obj); err != nil {
      return err
    }
  }

  return nil
}

func (c *client) delete(endpoint string, obj interface{}) error {
  req, err := c.newRequest(http.MethodDelete, endpoint)
  if err != nil {
    return err
  }

  resp, err := checkSuccessful(c.doRequest(req))
  if err != nil {
    return err
  }
  defer safeRespClose(resp)
  
  if obj != nil {
    if err := decodeJsonBody(resp, &obj); err != nil {
      return err
    }
  }

  return nil
}


type request struct {
  method string
  url *url.URL
  params url.Values

  obj interface{}
  body io.Reader
}

func (r *request) toHTTP() (*http.Request, error) {
  r.url.RawQuery = r.params.Encode()
  
  // add a request body if there is one
  if r.body == nil && r.obj != nil {
    body, err := encodeJsonBody(r.obj)
    if err != nil {
      return nil, err
    }
    r.body = body
  }

  req, err := http.NewRequest(r.method, r.url.RequestURI(), r.body)
  if err != nil {
    return nil, err
  }

  req.URL.Host = r.url.Host
  req.URL.Scheme = r.url.Scheme
  req.Host = r.url.Host
  return req, nil
}


func (c *client) newRequest(method, path string) (*request, error) {
  base, _ := url.Parse(c.address)
  u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
  req := &request{
    method: method,
    url: &url.URL{
      Scheme: base.Scheme,
      User: base.User,
      Host: base.Host,
      Path: endpoint(base.Path, u.Path),
    },
    params: make(url.Values),
  }
  return req, nil
}

// TODO fix based on version
func (c *client) useragent() string {
  return fmt.Sprintf("PulsarAdmin/2.2.0 (go)")
}

func (c *client) doRequest(r *request) (*http.Response, error) {
  req, err := r.toHTTP()
  if err != nil {
    return nil, err
  }

  // add default headers
  req.Header.Set("Content-Type", "application/json")
  req.Header.Set("Accept", "application/json")
  req.Header.Set("User-Agent", c.useragent())

  hc := c.httpClient
  if hc == nil {
    hc = http.DefaultClient
  }

  resp, err := hc.Do(req)
  return resp, err
}


// decodeJsonBody is used to JSON encode a body
func encodeJsonBody(obj interface{}) (io.Reader, error) {
  buf := bytes.NewBuffer(nil)
  enc := json.NewEncoder(buf)
  if err := enc.Encode(obj); err != nil {
    return nil, err
  }
  return buf, nil
}


// decodeJsonBody is used to JSON decode a body
func decodeJsonBody(resp *http.Response, out interface{}) error {
	dec := json.NewDecoder(resp.Body)
	return dec.Decode(out)
}

// safeRespClose is used to close a respone body
func safeRespClose(resp *http.Response) {
  if resp != nil {
    resp.Body.Close()
  }
}

// responseError is used to parse a response into a pulsar error
func responseError(resp *http.Response) error  {
  var e Error
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    e.Reason = err.Error()
    e.Code = resp.StatusCode
    return e
  }

  json.Unmarshal(body, &e)
  e.Code = resp.StatusCode

  if e.Reason == "" {
    e.Reason = unknownErrorReason
  }

  return e
}

// respIsOk is used to validate a successful http status code
func respIsOk(resp *http.Response) bool {
  return resp.StatusCode >= http.StatusOK && resp.StatusCode <= http.StatusNoContent
}

// checkSuccessful checks for a valid response and parses an error
func checkSuccessful(resp *http.Response, err error) (*http.Response, error) {
  if err != nil {
    safeRespClose(resp)
    return nil, err
  }

  if !respIsOk(resp) {
    defer safeRespClose(resp)
    return nil, responseError(resp)
  }

  return resp, nil
}

func endpoint(parts ...string) string {
  return path.Join(parts...)
}
