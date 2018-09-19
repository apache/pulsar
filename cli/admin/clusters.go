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


// Clusters  is used to access the cluster endpoints.
type Clusters interface {
  List() ([]string, error)
  Get(string) (ClusterData, error)
  Create(ClusterData) error
  Delete(string) error
}

type clusters struct {
  client *client
  basepath string
}

func (c *client) Clusters() Clusters {
  return &clusters{client:c, basepath:"/admin/v2/clusters"}
}

func (c *clusters) List() ([]string, error) {
  var clusters []string
  err := c.client.get(c.basepath, &clusters)
  return clusters, err
}

func (c *clusters) Get(name string) (ClusterData, error) {
  cdata := ClusterData{}
  endpoint := endpoint(c.basepath, name)
  err := c.client.get(endpoint, &cdata)
  return cdata, err
}

func (c *clusters) Create(cdata ClusterData) error {
  endpoint := endpoint(c.basepath, cdata.Name)
  err := c.client.put(endpoint, &cdata, nil)
  return err
}

func (c *clusters) Delete(name string) error {
  endpoint := endpoint(c.basepath, name)
  return c.client.delete(endpoint, nil)
}
