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
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestWriteElements(t *testing.T) {
	tests := []struct {
		in  interface{}
		buf []byte
	}{
		{int8(1), []byte{0x01}},
		{uint8(2), []byte{0x02}},
		{int16(4), []byte{0x04, 0x00}},
		{uint16(16), []byte{0x10, 0x00}},
		{int32(1), []byte{0x01, 0x00, 0x00, 0x00}},
		{uint32(256), []byte{0x00, 0x01, 0x00, 0x00}},
		{
			int64(65536),
			[]byte{0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			uint64(4294967296),
			[]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00},
		},
		{
			true,
			[]byte{0x01},
		},
		{
			false,
			[]byte{0x00},
		},
	}

	t.Logf("Running %d tests", len(tests))
	for i, test := range tests {
		value := test

		var buf bytes.Buffer
		err := WriteElements(&buf, value.in)
		if err != nil {
			t.Errorf("writeElement #%d error %v", i, err)
			continue
		}
		if !bytes.Equal(buf.Bytes(), test.buf) {
			t.Error(test.in)
			t.Errorf("writeElement #%d\n got: %s want: %s", i,
				spew.Sdump(buf.Bytes()), spew.Sdump(test.buf))
			continue
		}

		// Read from wire format.
		rbuf := bytes.NewReader(test.buf)
		val := test.in
		if reflect.ValueOf(test.in).Kind() != reflect.Ptr {
			val = reflect.New(reflect.TypeOf(test.in)).Interface()
		}
		err = ReadElements(rbuf, val)
		if err != nil {
			t.Errorf("readElement #%d error %v", i, err)
			continue
		}
		ival := val
		if reflect.ValueOf(test.in).Kind() != reflect.Ptr {
			ival = reflect.Indirect(reflect.ValueOf(val)).Interface()
		}
		if !reflect.DeepEqual(ival, test.in) {
			t.Errorf("readElement #%d\n got: %s want: %s", i,
				spew.Sdump(ival), spew.Sdump(test.in))
			continue
		}
	}
}

func TestElementErrors(t *testing.T) {
	tests := []struct {
		in       interface{}
		max      int
		writeErr error
		readErr  error
	}{
		{int8(1), 0, nil, io.EOF},
		{uint8(2), 0, nil, io.EOF},
		{int16(4), 0, nil, io.EOF},
		{uint16(16), 0, nil, io.EOF},
		{int32(1), 0, nil, io.EOF},
		{uint32(256), 0, nil, io.EOF},
		{
			int64(65536),
			0, nil, io.EOF,
		},
		{
			uint64(4294967296),
			0, nil, io.EOF,
		},
		{
			true,
			0, nil, io.EOF,
		},
		{
			false,
			0, nil, io.EOF,
		},
	}

	t.Logf("Running %d tests", len(tests))
	for i, test := range tests {
		var r bytes.Reader
		val := test.in
		if reflect.ValueOf(test.in).Kind() != reflect.Ptr {
			val = reflect.New(reflect.TypeOf(test.in)).Interface()
		}
		err := ReadElements(&r, val)
		if err != test.readErr {
			t.Errorf("readElement #%d wrong error got: %v, want: %v",
				i, err, test.readErr)
			continue
		}
	}
}
