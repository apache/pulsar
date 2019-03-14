/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package instance

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/apache/pulsar/pulsar-function-go/log"
)

type SerDe interface {
	Serialize(input interface{}) []byte
	Deserialize(input []byte) interface{}
}

type IdentitySerDe struct {
}

func (isd *IdentitySerDe) Serialize(input interface{}) []byte {
	buf := bytes.Buffer{}
	ser := gob.NewEncoder(&buf)
	err := ser.Encode(input)
	if err != nil {
		log.Errorf("failed gob Encode, %v", err)
	}
	return buf.Bytes()
}

func (isd *IdentitySerDe) Deserialize(input []byte) interface{} {
	buf := bytes.Buffer{}
	buf.Write(input)
	de := gob.NewDecoder(&buf)
	err := de.Decode(&buf)
	if err != nil {
		log.Errorf("failed gob Decode, %v", err)
	}
	return buf
}

func init() {
	gob.Register(IdentitySerDe{})
}

func GetProperties(fullyQualifiedName string, instanceId int) map[string]string {
	propertiesMap := make(map[string]string)
	propertiesMap["application"] = "pulsar-function"
	propertiesMap["id"] = fullyQualifiedName
	propertiesMap["instance_id"] = fmt.Sprintf("%d", instanceId)

	return propertiesMap
}

func GetDefaultSubscriptionName(tenant, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", tenant, namespace, name)
}

func GetFullyQualifiedInstanceId(tenant, namespace, name string, instanceID int) string {
	return fmt.Sprintf("%s/%s/%s:%d", tenant, namespace, name, instanceID)
}
