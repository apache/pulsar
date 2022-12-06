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

//
// This file borrows some of the implementations from
// {@link https://github.com/aws/aws-lambda-go/blob/master/lambda/function_test.go}
//  - TestInvalidFunctions
//

package pf

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint
func TestInvalidFunctions(t *testing.T) {

	testCases := []struct {
		name     string
		function interface{}
		expected error
	}{
		{
			name:     "nil function",
			expected: errors.New("function is nil"),
			function: nil,
		},
		{
			name:     "function is not a function",
			expected: errors.New("function kind struct is not func"),
			function: struct{}{},
		},
		{
			name:     "function declares too many arguments",
			expected: errors.New("functions may not take more than two arguments, but function takes 3"),
			function: func(n context.Context, x string, y string) error {
				return nil
			},
		},
		{
			name:     "two argument function does not context as first argument",
			expected: errors.New("function takes two arguments, but the first is not Context. got string"),
			function: func(a string, x context.Context) error {
				return nil
			},
		},
		{
			name:     "function returns too many values",
			expected: errors.New("function may not return more than two values"),
			function: func() (error, error, error) {
				return nil, nil, nil
			},
		},
		{
			name:     "function returning two values does not declare error as the second return value",
			expected: errors.New("function returns two values, but the second does not implement error"),
			function: func() (error, string) {
				return nil, "hello"
			},
		},
		{
			name:     "function returning a single value does not implement error",
			expected: errors.New("function returns a single value, but it does not implement error"),
			function: func() string {
				return "hello"
			},
		},
		{
			name:     "no return value should not result in error",
			expected: nil,
			function: func() {
			},
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("testCase[%d] %s", i, testCase.name), func(t *testing.T) {
			pulsarFunction := newFunction(testCase.function)
			_, err := pulsarFunction.process(context.TODO(), make([]byte, 0))
			assert.Equal(t, testCase.expected, err)
		})
	}
}

type expected struct {
	val []byte
	err error
}

var (
	input = []byte{102, 117, 110, 99, 116, 105, 111, 110}
)

func TestInvokes(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected expected
		function interface{}
	}{
		{
			input:    input,
			expected: expected{input, nil},
			function: func(in []byte) ([]byte, error) {
				return input, nil
			},
		},
		{
			input:    input,
			expected: expected{input, nil},
			function: func(in []byte) ([]byte, error) {
				return input, nil
			},
		},
		{
			input:    input,
			expected: expected{input, nil},
			function: func(ctx context.Context, in []byte) ([]byte, error) {
				return input, nil
			},
		},
		{
			input:    input,
			expected: expected{nil, errors.New("bad stuff")},
			function: func() error {
				return errors.New("bad stuff")
			},
		},
		{
			input:    input,
			expected: expected{nil, errors.New("bad stuff")},
			function: func() ([]byte, error) {
				return nil, errors.New("bad stuff")
			},
		},
		{
			input:    input,
			expected: expected{nil, errors.New("bad stuff")},
			function: func(e []byte) ([]byte, error) {
				return nil, errors.New("bad stuff")
			},
		},
		{
			input:    input,
			expected: expected{nil, errors.New("bad stuff")},
			function: func(ctx context.Context, e []byte) ([]byte, error) {
				return nil, errors.New("bad stuff")
			},
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("testCase[%d] %s", i, testCase.name), func(t *testing.T) {
			pulsarFunction := newFunction(testCase.function)
			response, err := pulsarFunction.process(context.TODO(), testCase.input)
			if testCase.expected.err != nil {
				assert.Equal(t, testCase.expected.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expected.val, response)
			}
		})
	}
}
