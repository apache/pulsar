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
	"context"
	_ "embed"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	cfg "github.com/apache/pulsar/pulsar-function-go/conf"
	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// workerInstanceConf is the config that the function worker passes to a Go function with secrets
// on Kubernetes: RuntimeUtilsTest.goInstanceConfIsTheOneTheGoInstanceIsTestedWith, on the Java
// side, checks that the worker builds exactly this file.
//
//go:embed testdata/go-instance-conf.json
var workerInstanceConf []byte

// newFuncContextFromWorkerConf builds the function context from the JSON the function worker
// passes in -instance-conf, as conf.GetConf does.
func newFuncContextFromWorkerConf(t *testing.T, instanceConfJSON string) *FunctionContext {
	conf := &cfg.Conf{}
	require.NoError(t, json.Unmarshal([]byte(instanceConfJSON), conf))
	return newFuncContextWithConf(newInstanceConfWithConf(conf))
}

// The instance is compiled into the function binary and reads its config from the JSON that the
// worker builds, so the two sides are tested against one file. A change to one side that the other
// does not follow fails this test or the Java one.
func TestInstanceUnderstandsTheConfOfTheWorker(t *testing.T) {
	t.Setenv("DB_PASSWORD", "password-from-env")
	t.Setenv("DB_USER", "user-from-env")
	fc := newFuncContextFromWorkerConf(t, string(workerInstanceConf))

	assert.Equal(t, "public/default/go-secrets", fc.GetTenantAndNamespaceAndName())
	assert.Equal(t, []string{"persistent://public/default/go-secrets-in"}, fc.GetInputTopics())
	assert.Equal(t, "persistent://public/default/go-secrets-out", fc.GetOutputTopic())
	assert.Equal(t, "value", fc.GetUserConfValue("key"))

	// the worker selected the environment based provider, so the secrets come from the environment
	for secretName, expected := range map[string]string{
		"DB_PASSWORD": "password-from-env",
		"DB_USER":     "user-from-env",
	} {
		secret, err := fc.GetSecret(context.Background(), secretName)
		assert.NoError(t, err)
		assert.Equal(t, expected, secret)
	}
}

func TestGetSecret_providerOfTheWorker(t *testing.T) {
	t.Setenv("PF_TEST_SECRET", "from-env")
	testCases := []struct {
		className string
		expected  string
	}{
		// a worker that predates secrets support in Go sends no name
		{"", "clear"},
		{"ClearTextSecretsProvider", "clear"},
		{"EnvironmentBasedSecretsProvider", "from-env"},
		// a custom SecretsProviderConfigurator can send every runtime the name of the Python or Java class
		{"secretsprovider.ClearTextSecretsProvider", "clear"},
		{"secretsprovider.EnvironmentBasedSecretsProvider", "from-env"},
		{"org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider", "clear"},
		{"org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider", "from-env"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.className, func(t *testing.T) {
			conf := &cfg.Conf{
				AutoACK:                  true,
				SecretsProviderClassName: testCase.className,
				SecretsMap:               `{"PF_TEST_SECRET": "clear"}`,
			}
			fc := newFuncContextWithConf(newInstanceConfWithConf(conf))
			log.DrainEntries()

			secret, err := fc.GetSecret(context.Background(), "PF_TEST_SECRET")
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, secret)
			assert.NotContains(t, strings.Join(log.DrainEntries(), "\n"), "unknown secrets provider")
		})
	}
}

// A provider that the instance does not know must not stop the function: it falls back to clear
// text and says so, once.
func TestGetSecret_unknownProviderFallsBackToClearText(t *testing.T) {
	fc := newFuncContextFromWorkerConf(t, `{
		"autoAck": true,
		"userConfig": "{\"key\": \"value\"}",
		"secretsProviderClassName": "com.example.VaultSecretsProvider",
		"secretsMap": "{\"DB_PASSWORD\": \"db@DB_PASSWORD\"}"
	}`)
	log.DrainEntries()

	// nothing is looked at before a secret is read
	assert.Equal(t, "value", fc.GetUserConfValue("key"))
	assert.NotContains(t, strings.Join(log.DrainEntries(), "\n"), "unknown secrets provider")

	var readers sync.WaitGroup
	for i := 0; i < 20; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			secret, err := fc.GetSecret(context.Background(), "DB_PASSWORD")
			assert.NoError(t, err)
			assert.Equal(t, "db@DB_PASSWORD", secret)
		}()
	}
	readers.Wait()

	const warning = `unknown secrets provider "com.example.VaultSecretsProvider", ` +
		`falling back to ClearTextSecretsProvider`
	assert.Equal(t, 1, strings.Count(strings.Join(log.DrainEntries(), "\n"), warning))
}

func TestGetSecret_clearText(t *testing.T) {
	fc := newFuncContextFromWorkerConf(t, `{
		"autoAck": true,
		"secretsProviderClassName": "ClearTextSecretsProvider",
		"secretsMap": "{\"text\": \"cake is a lie\", \"empty\": \"\", \"pin\": 12345678901234567890,`+
		` \"ratio\": 1.10, \"flag\": true, \"ref\": {\"path\": \"db\", \"key\": \"password\"}, \"null\": null}"
	}`)

	for secretName, expected := range map[string]string{
		"text":  "cake is a lie",
		"empty": "",
		// a number is the secret as it was written, not a float
		"pin":   "12345678901234567890",
		"ratio": "1.10",
		"flag":  "true",
		"ref":   `{"path": "db", "key": "password"}`,
		"null":  "",
	} {
		secret, err := fc.GetSecret(context.Background(), secretName)
		assert.NoError(t, err, secretName)
		assert.Equal(t, expected, secret, secretName)
	}

	_, err := fc.GetSecret(context.Background(), "not-a-secret")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

func TestGetSecret_environmentBased(t *testing.T) {
	fc := newFuncContextFromWorkerConf(t, `{
		"autoAck": true,
		"secretsProviderClassName": "EnvironmentBasedSecretsProvider",
		"secretsMap": "{\"PF_TEST_SECRET\": {\"path\": \"db\", \"key\": \"password\"},`+
		` \"PF_TEST_EMPTY_SECRET\": {\"path\": \"db\", \"key\": \"empty\"},`+
		` \"PF_TEST_SECRET_THAT_IS_NOT_SET\": {\"path\": \"db\", \"key\": \"user\"}}"
	}`)
	t.Setenv("PF_TEST_SECRET", "from-env")
	t.Setenv("PF_TEST_EMPTY_SECRET", "")
	t.Setenv("PF_TEST_NOT_A_SECRET", "must not be readable")

	secret, err := fc.GetSecret(context.Background(), "PF_TEST_SECRET")
	assert.NoError(t, err)
	assert.Equal(t, "from-env", secret)

	secret, err = fc.GetSecret(context.Background(), "PF_TEST_EMPTY_SECRET")
	assert.NoError(t, err)
	assert.Equal(t, "", secret)

	_, err = fc.GetSecret(context.Background(), "PF_TEST_SECRET_THAT_IS_NOT_SET")
	assert.ErrorIs(t, err, ErrSecretNotFound)

	// only the secrets the function was given are served, not the whole environment
	_, err = fc.GetSecret(context.Background(), "PF_TEST_NOT_A_SECRET")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

func TestGetSecret_secretsMapFromFunctionDetails(t *testing.T) {
	fc := newFuncContextFromWorkerConf(t, `{
		"secretsProviderClassName": "ClearTextSecretsProvider",
		"functionDetails": "{\"autoAck\": true, \"secretsMap\": \"{\\\"password\\\": \\\"cake is a lie\\\"}\"}"
	}`)

	secret, err := fc.GetSecret(context.Background(), "password")
	assert.NoError(t, err)
	assert.Equal(t, "cake is a lie", secret)
}

func TestGetSecret_withoutSecrets(t *testing.T) {
	fc := newFuncContextFromWorkerConf(t, `{"autoAck": true}`)

	_, err := fc.GetSecret(context.Background(), "password")
	assert.ErrorIs(t, err, ErrSecretNotFound)

	// a context that was not built from the config of an instance has no secrets either
	_, err = (&FunctionContext{}).GetSecret(context.Background(), "password")
	assert.ErrorIs(t, err, ErrSecretNotFound)
}

func TestGetSecret_secretsMapIsNotAnObject(t *testing.T) {
	fc := newFuncContextFromWorkerConf(t, `{"autoAck": true, "secretsMap": "password=cake"}`)

	_, err := fc.GetSecret(context.Background(), "password")
	assert.ErrorContains(t, err, "the secrets map of the function is not a JSON object")
	// it must not look as if the function had no such secret, nor repeat the secrets map
	assert.NotErrorIs(t, err, ErrSecretNotFound)
	assert.NotContains(t, err.Error(), "cake")
}

func TestNewFuncContext_contextsShareTheSecrets(t *testing.T) {
	assert.Same(t, NewFuncContext().secrets, NewFuncContext().secrets)
}
