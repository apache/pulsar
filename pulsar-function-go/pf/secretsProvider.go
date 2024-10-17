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
	"os"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
)

type SecretsProvider interface {
	GetValue(secrets map[string]interface{}, key string) interface{}
}

type ClearTextSecretsProvider struct{}

func (p *ClearTextSecretsProvider) GetValue(secrets map[string]interface{}, key string) interface{} {
	val, ok := secrets[key]
	if !ok {
		log.Debugf("secret key %s not present in function secrets", key)
		return nil
	}
	return val
}

type EnvironmentBasedSecretsProvider struct{}

func (p *EnvironmentBasedSecretsProvider) GetValue(_ map[string]interface{}, key string) interface{} {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Debugf("secret key %s does not match an environment variable", key)
		return nil
	}
	return val
}
