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
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	IoMaxSize     = 1024
	maxBorrowSize = 10
)

var (
	bigEndian = binary.BigEndian
)

type BinaryFreeList chan []byte

var BinarySerializer BinaryFreeList = make(chan []byte, IoMaxSize)

func (b BinaryFreeList) Borrow() (buf []byte) {
	select {
	case buf = <-b:
	default:
		buf = make([]byte, maxBorrowSize)

	}
	return buf[:maxBorrowSize]
}

func (b BinaryFreeList) Return(buf []byte) {
	select {
	case b <- buf:
	default:
	}
}

func (b BinaryFreeList) Uint8(r io.Reader) (uint8, error) {
	buf := b.Borrow()[:1]
	if _, err := io.ReadFull(r, buf); err != nil {
		b.Return(buf)
		return 0, err
	}
	rv := buf[0]
	b.Return(buf)
	return rv, nil
}

func (b BinaryFreeList) Uint16(r io.Reader, byteOrder binary.ByteOrder) (uint16, error) {
	buf := b.Borrow()[:2]
	if _, err := io.ReadFull(r, buf); err != nil {
		b.Return(buf)
		return 0, err
	}
	rv := byteOrder.Uint16(buf)
	b.Return(buf)
	return rv, nil
}

func (b BinaryFreeList) Uint32(r io.Reader, byteOrder binary.ByteOrder) (uint32, error) {
	buf := b.Borrow()[:4]
	if _, err := io.ReadFull(r, buf); err != nil {
		b.Return(buf)
		return 0, err
	}
	rv := byteOrder.Uint32(buf)
	b.Return(buf)
	return rv, nil
}

func (b BinaryFreeList) Uint64(r io.Reader, byteOrder binary.ByteOrder) (uint64, error) {
	buf := b.Borrow()[:8]
	if _, err := io.ReadFull(r, buf); err != nil {
		b.Return(buf)
		return 0, err
	}
	rv := byteOrder.Uint64(buf)
	b.Return(buf)
	return rv, nil
}

func (b BinaryFreeList) Float64(buf []byte) (float64, error) {
	if len(buf) < 8 {
		return 0, fmt.Errorf("cannot decode binary double: %s", io.ErrShortBuffer)
	}
	return math.Float64frombits(binary.BigEndian.Uint64(buf[:8])), nil
}

func (b BinaryFreeList) Float32(buf []byte) (float32, error) {
	if len(buf) < 4 {
		return 0, fmt.Errorf("cannot decode binary float: %s", io.ErrShortBuffer)
	}
	return math.Float32frombits(binary.BigEndian.Uint32(buf[:4])), nil
}

func (b BinaryFreeList) PutUint8(w io.Writer, val uint8) error {
	buf := b.Borrow()[:1]
	buf[0] = val
	_, err := w.Write(buf)
	b.Return(buf)
	return err
}

func (b BinaryFreeList) PutUint16(w io.Writer, byteOrder binary.ByteOrder, val uint16) error {
	buf := b.Borrow()[:2]
	byteOrder.PutUint16(buf, val)
	_, err := w.Write(buf)
	b.Return(buf)
	return err
}

func (b BinaryFreeList) PutUint32(w io.Writer, byteOrder binary.ByteOrder, val uint32) error {

	buf := b.Borrow()[:4]
	byteOrder.PutUint32(buf, val)
	_, err := w.Write(buf)
	b.Return(buf)
	return err
}

func (b BinaryFreeList) PutUint64(w io.Writer, byteOrder binary.ByteOrder, val uint64) error {
	buf := b.Borrow()[:8]
	byteOrder.PutUint64(buf, val)
	_, err := w.Write(buf)
	b.Return(buf)
	return err
}

func (b BinaryFreeList) PutDouble(datum interface{}) ([]byte, error) {
	var value float64
	switch v := datum.(type) {
	case float64:
		value = v
	case float32:
		value = float64(v)
	case int:
		if value = float64(v); int(value) != v {
			return nil, fmt.Errorf("serialize failed: provided Go int would lose precision: %d", v)
		}
	case int64:
		if value = float64(v); int64(value) != v {
			return nil, fmt.Errorf("serialize failed: provided Go int64 would lose precision: %d", v)
		}
	case int32:
		if value = float64(v); int32(value) != v {
			return nil, fmt.Errorf("serialize failed: provided Go int32 would lose precision: %d", v)
		}
	default:
		return nil, fmt.Errorf("serialize failed: expected: Go numeric; received: %T", datum)
	}
	var buf []byte
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[len(buf)-8:], math.Float64bits(value))
	return buf, nil
}

func (b BinaryFreeList) PutFloat(datum interface{}) ([]byte, error) {
	var value float32
	switch v := datum.(type) {
	case float32:
		value = v
	case float64:
		value = float32(v)
	case int:
		if value = float32(v); int(value) != v {
			return nil, fmt.Errorf("serialize failed: provided Go int would lose precision: %d", v)
		}
	case int64:
		if value = float32(v); int64(value) != v {
			return nil, fmt.Errorf("serialize failed: provided Go int64 would lose precision: %d", v)
		}
	case int32:
		if value = float32(v); int32(value) != v {
			return nil, fmt.Errorf("serialize failed: provided Go int32 would lose precision: %d", v)
		}
	default:
		return nil, fmt.Errorf("serialize failed: expected: Go numeric; received: %T", datum)
	}
	var buf []byte
	buf = append(buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(buf[len(buf)-4:], uint32(math.Float32bits(value)))
	return buf, nil
}

func ReadElements(r io.Reader, elements ...interface{}) error {
	for _, element := range elements {
		err := readElement(r, element)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteElements(w io.Writer, elements ...interface{}) error {
	for _, element := range elements {
		err := writeElement(w, element)
		if err != nil {
			return err
		}
	}
	return nil
}

func readElement(r io.Reader, element interface{}) error {
	switch e := element.(type) {
	case *int8:
		rv, err := BinarySerializer.Uint8(r)
		if err != nil {
			return err
		}
		*e = int8(rv)
		return nil

	case *int16:
		rv, err := BinarySerializer.Uint16(r, bigEndian)
		if err != nil {
			return err
		}
		*e = int16(rv)
		return nil

	case *int32:
		rv, err := BinarySerializer.Uint32(r, bigEndian)
		if err != nil {
			return err
		}
		*e = int32(rv)
		return nil

	case *int64:
		rv, err := BinarySerializer.Uint64(r, bigEndian)
		if err != nil {
			return err
		}
		*e = int64(rv)
		return nil

	case *bool:
		rv, err := BinarySerializer.Uint8(r)
		if err != nil {
			return err
		}
		if rv == 0x00 {
			*e = false
		} else {
			*e = true
		}
		return nil
	}
	return binary.Read(r, bigEndian, element)
}

func writeElement(w io.Writer, element interface{}) error {
	switch e := element.(type) {
	case int8:
		err := BinarySerializer.PutUint8(w, uint8(e))
		if err != nil {
			return err
		}
		return nil

	case int16:
		err := BinarySerializer.PutUint16(w, bigEndian, uint16(e))
		if err != nil {
			return err
		}
		return nil

	case int32:
		err := BinarySerializer.PutUint32(w, bigEndian, uint32(e))
		if err != nil {
			return err
		}
		return nil

	case int64:
		err := BinarySerializer.PutUint64(w, bigEndian, uint64(e))
		if err != nil {
			return err
		}
		return nil

	case bool:
		var err error
		if e {
			err = BinarySerializer.PutUint8(w, 0x01)
		} else {
			err = BinarySerializer.PutUint8(w, 0x00)
		}
		if err != nil {
			return err
		}
		return nil
	}
	return binary.Write(w, bigEndian, element)
}
