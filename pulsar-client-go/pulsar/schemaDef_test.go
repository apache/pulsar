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

package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaDef(t *testing.T) {
	errSchemaDef := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"int64\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	_, err := initAvroCodec(errSchemaDef)
	assert.NotNil(t, err)

	errSchemaDef1 := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"bool\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	_, err = initAvroCodec(errSchemaDef1)
	assert.NotNil(t, err)

	errSchemaDef2 := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"float32\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	_, err = initAvroCodec(errSchemaDef2)
	assert.NotNil(t, err)

	errSchemaDef3 := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"float64\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	_, err = initAvroCodec(errSchemaDef3)
	assert.NotNil(t, err)

	errSchemaDef4 := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"byte\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	_, err = initAvroCodec(errSchemaDef4)
	assert.NotNil(t, err)

	errSchemaDef5 := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"operation.createJsonConsumer\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"byte\"},{\"name\":\"Name\",\"type\":\":[\"null\",\"string\"],\"default\":null\"}]}"
	_, err = initAvroCodec(errSchemaDef5)
	assert.NotNil(t, err)
}
