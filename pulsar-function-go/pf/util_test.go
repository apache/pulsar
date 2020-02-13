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

package pf

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	tenant     = "pulsar"
	namespace  = "function"
	name       = "go"
	instanceID = 100
)

func TestUtils(t *testing.T) {
	fqfn := tenant + "/" + namespace + "/" + name

	propertiesMap := make(map[string]string)
	propertiesMap["application"] = "pulsar-function"
	propertiesMap["id"] = "pulsar/function/go"
	propertiesMap["instance_id"] = fmt.Sprintf("%d", instanceID)

	expectedFQFN := getDefaultSubscriptionName(tenant, namespace, name)
	assert.Equal(t, expectedFQFN, fqfn)

	actualtMap := getProperties(fqfn, 100)
	assert.Equal(t, propertiesMap, actualtMap)

	expectedRes := getFullyQualifiedInstanceID(tenant, namespace, name, instanceID)
	assert.Equal(t, expectedRes, "pulsar/function/go:100")
}
