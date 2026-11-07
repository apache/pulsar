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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAuthentication(t *testing.T) {
	tests := []struct {
		name       string
		authPlugin string
		authParams string
		wantErr    bool
	}{
		{name: "no auth plugin", authPlugin: authPluginNone},
		{name: "token", authPlugin: authPluginToken, authParams: "token:my-token"},
		{name: "token from file", authPlugin: authPluginToken, authParams: "file:///mnt/secrets/token"},
		{name: "token without prefix", authPlugin: authPluginToken, authParams: "my-token", wantErr: true},
		{name: "token without params", authPlugin: authPluginToken, wantErr: true},
		{
			// The worker passes the Java client params, where the type is optional
			name:       "oauth2 without type",
			authPlugin: authPluginOAuth2,
			authParams: `{"issuerUrl":"https://test.com","audience":"test-audience",` +
				`"privateKey":"file:///mnt/secrets/auth.json"}`,
		},
		{
			name:       "oauth2 with type",
			authPlugin: authPluginOAuth2,
			authParams: `{"type":"client_credentials","issuerUrl":"https://test.com",` +
				`"audience":"test-audience","scope":"test-scope","privateKey":"file:///mnt/secrets/auth.json"}`,
		},
		{
			name:       "oauth2 with unsupported type",
			authPlugin: authPluginOAuth2,
			authParams: `{"type":"authorization_code","issuerUrl":"https://test.com"}`,
			wantErr:    true,
		},
		{name: "oauth2 with invalid params", authPlugin: authPluginOAuth2, authParams: "issuerUrl:x", wantErr: true},
		{name: "oauth2 with empty params", authPlugin: authPluginOAuth2, authParams: "{}", wantErr: true},
		{name: "oauth2 without params", authPlugin: authPluginOAuth2, wantErr: true},
		{name: "unknown auth plugin", authPlugin: "com.MyAuth", authParams: "{}", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authentication, err := newAuthentication(&instanceConf{
				authPlugin: test.authPlugin,
				authParams: test.authParams,
			})
			if test.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			if test.authPlugin != authPluginNone {
				assert.NotNil(t, authentication)
			}
		})
	}
}
