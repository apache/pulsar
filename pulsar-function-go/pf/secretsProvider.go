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
	"encoding/json"
	"errors"
	"os"
	"sync"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
)

// ErrSecretNotFound is returned by FunctionContext.GetSecret when the function has no secret of
// the given name, or when its secrets provider has none for it.
var ErrSecretNotFound = errors.New("secret not found")

// secretsProvider resolves a secret that is in the secrets map of the function. pathToSecret is
// what the map holds for secretName: the string itself, the JSON text of a value that is not a
// string, or "" for null. What it means is up to the provider.
type secretsProvider interface {
	provideSecret(ctx context.Context, secretName, pathToSecret string) (string, error)
}

// clearTextSecretsProvider treats the value in the secrets map as the secret itself.
type clearTextSecretsProvider struct{}

func (clearTextSecretsProvider) provideSecret(_ context.Context, _, pathToSecret string) (string, error) {
	return pathToSecret, nil
}

// environmentBasedSecretsProvider reads the secret from the environment variable named after it.
// That is where the Kubernetes runtime puts the secrets of a function.
type environmentBasedSecretsProvider struct{}

func (environmentBasedSecretsProvider) provideSecret(_ context.Context, secretName, _ string) (string, error) {
	secret, ok := os.LookupEnv(secretName)
	if !ok {
		return "", ErrSecretNotFound
	}
	return secret, nil
}

// newSecretsProvider returns the secrets provider that the function worker selected.
//
// The instance is compiled into the function binary, so it has to work with any worker. A worker
// that predates secrets support in Go sends no class name, and a worker with a custom
// SecretsProviderConfigurator can send every runtime the same class name, which is then the name
// of a Java or Python class. The Java and Python names of the two providers are therefore
// accepted, and any other name falls back to clear text, with a warning, instead of stopping the
// function.
func newSecretsProvider(className string) secretsProvider {
	switch className {
	case "EnvironmentBasedSecretsProvider",
		"secretsprovider.EnvironmentBasedSecretsProvider",
		"org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider":
		return environmentBasedSecretsProvider{}
	case "",
		"ClearTextSecretsProvider",
		"secretsprovider.ClearTextSecretsProvider",
		"org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider":
		return clearTextSecretsProvider{}
	default:
		log.Warnf("unknown secrets provider %q, falling back to ClearTextSecretsProvider", className)
		return clearTextSecretsProvider{}
	}
}

// functionSecrets reads the secrets of a function through the secrets provider that the function
// worker selected. Nothing is parsed before the first secret is read, so a function that reads
// none is not affected.
type functionSecrets struct {
	className  string // secrets provider the function worker selected, "" for none
	secretsMap string // the secrets map of the function, a JSON object, or ""

	load     sync.Once
	provider secretsProvider
	secrets  map[string]json.RawMessage
	err      error
}

func newFunctionSecrets(instanceConf *instanceConf) *functionSecrets {
	return &functionSecrets{
		className:  instanceConf.secretsProviderClassName,
		secretsMap: instanceConf.funcDetails.GetSecretsMap(),
	}
}

func (s *functionSecrets) get(ctx context.Context, secretName string) (string, error) {
	if s == nil {
		return "", ErrSecretNotFound
	}
	s.load.Do(func() {
		s.provider = newSecretsProvider(s.className)
		if s.secretsMap == "" {
			return
		}
		if err := json.Unmarshal([]byte(s.secretsMap), &s.secrets); err != nil {
			// The error does not repeat the secrets map: under the clear text provider it is
			// the secrets.
			s.err = errors.New("the secrets map of the function is not a JSON object")
		}
	})
	if s.err != nil {
		return "", s.err
	}
	value, ok := s.secrets[secretName]
	if !ok {
		return "", ErrSecretNotFound
	}
	var pathToSecret string
	if err := json.Unmarshal(value, &pathToSecret); err != nil {
		// not a string: the provider gets the JSON text, which keeps a number as it was written
		pathToSecret = string(value)
	}
	return s.provider.provideSecret(ctx, secretName, pathToSecret)
}
