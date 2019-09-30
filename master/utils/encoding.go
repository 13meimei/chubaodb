// Copyright 2019 The ChuBao Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package utils

import (
	"errors"
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/utils/encoding"
	"github.com/chubaodb/chubaodb/master/utils/hack"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"strconv"

	"encoding/binary"
)

const (
	Store_Prefix_Invalid uint8 = 0

	//record_data
	Store_Prefix_KV    uint8 = 1
	Store_Prefix_INDEX uint8 = 2

	//meta_data
	Store_Prefix_Range         uint8 = 2
	Store_Prefix_RaftLog       uint8 = 3
	Store_Prefix_Node          uint8 = 4
	Store_Prefix_Range_Version uint8 = 5
)

func EncodeStorePrefix(prefix uint8, id uint64) (start []byte, end []byte) {
	return toByte(prefix, id), toByte(prefix, id+1)
}

func toByte(prefix uint8, id uint64) []byte {
	var buff []byte
	buff = make([]byte, 9)
	buff[0] = byte(prefix)
	binary.BigEndian.PutUint64(buff[1:], id)
	return buff
}

func DecodeStorePrefix(buff []byte) (uint8, uint64, error) {
	if len(buff) < 9 {
		return 0, 0, errors.New("invalid param")
	}
	prefix := uint8(buff[0])
	return prefix, binary.BigEndian.Uint64(buff[1:]), nil
}

//The EncodeColumnValue coded column lists the ID before the value
// Note: no sorting property is maintained after encoding (that is, if a > b, then the encoded byte array bytes.Compare(encA, encB) >0 is not necessarily true)
func EncodeColumnValue(buf []byte, col *basepb.Column, sval []byte) ([]byte, error) {
	log.Debug("---column %v: %v", col.GetName(), sval)
	if len(sval) == 0 {
		return buf, nil
	}
	switch col.DataType {
	case basepb.DataType_Tinyint, basepb.DataType_Smallint, basepb.DataType_Int, basepb.DataType_BigInt:
		if col.Unsigned {
			ival, err := strconv.ParseUint(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse unsigned integer failed(%s) when encoding column(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeIntValue(buf, uint32(col.Id), int64(ival)), nil
		} else {
			ival, err := strconv.ParseInt(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse integer failed(%s) when encoding column(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeIntValue(buf, uint32(col.Id), ival), nil
		}
	case basepb.DataType_Float, basepb.DataType_Double:
		fval, err := strconv.ParseFloat(hack.String(sval), 64)
		if err != nil {
			return nil, fmt.Errorf("parse float failed(%s) when encoding column(%s)", err.Error(), col.Name)
		}
		return encoding.EncodeFloatValue(buf, uint32(col.Id), fval), nil
	case basepb.DataType_Varchar, basepb.DataType_Binary, basepb.DataType_Date, basepb.DataType_TimeStamp:
		return encoding.EncodeBytesValue(buf, uint32(col.Id), sval), nil
	default:
		return nil, fmt.Errorf("unsupported type(%s) when encoding column(%s)", col.DataType.String(), col.Name)
	}
}

func EncodeValue2(buf []byte, colId uint32, typ encoding.Type, sval []byte) ([]byte, error) {
	log.Debug("---column type:%v colId:%v: sval:%v", typ, colId, sval)
	if len(sval) == 0 {
		return buf, nil
	}
	switch typ {
	case encoding.Int:
		ival, err := strconv.ParseInt(hack.String(sval), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse integer failed(%s) when encoding column(%d) ", err.Error(), colId)
		}
		return encoding.EncodeIntValue(buf, colId, ival), nil
	case encoding.Float:
		fval, err := strconv.ParseFloat(hack.String(sval), 64)
		if err != nil {
			return nil, fmt.Errorf("parse float failed(%s) when encoding column(%d)", err.Error(), colId)
		}
		return encoding.EncodeFloatValue(buf, colId, fval), nil
	case encoding.Bytes:
		return encoding.EncodeBytesValue(buf, colId, sval), nil
	default:
		return nil, fmt.Errorf("unsupported type(%s) when encoding column(%d)", typ, colId)
	}
}

// DecodeColumnValue
func DecodeColumnValue(buf []byte, col *basepb.Column) ([]byte, interface{}, error) {
	// check Null
	_, _, _, typ, err := encoding.DecodeValueTag(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode value tag for column(%v) failed(%v)", col.Name, err)
	}
	if typ == encoding.Null {
		_, length, err := encoding.PeekValueLength(buf)
		if err != nil {
			return nil, nil, fmt.Errorf("decode null value length for column(%v) failed(%v)", col.Name, err)
		}
		return buf[length:], nil, nil
	}

	switch col.DataType {
	case basepb.DataType_Tinyint, basepb.DataType_Smallint, basepb.DataType_Int, basepb.DataType_BigInt:
		remainBuf, ival, err := encoding.DecodeIntValue(buf)
		if col.Unsigned {
			return remainBuf, uint64(ival), err
		} else {
			return remainBuf, ival, err
		}
	case basepb.DataType_Float, basepb.DataType_Double:
		return encoding.DecodeFloatValue(buf)
	case basepb.DataType_Varchar, basepb.DataType_Binary, basepb.DataType_Date, basepb.DataType_TimeStamp:
		return encoding.DecodeBytesValue(buf)
	default:
		return nil, nil, fmt.Errorf("unsupported type(%s) when decoding column(%s)", col.DataType.String(), col.Name)
	}
}

func DecodeValue2(buf []byte) ([]byte, uint32, []byte, encoding.Type, error) {
	// check Null
	_, _, colId, typ, err := encoding.DecodeValueTag(buf)
	if err != nil {
		return nil, 0, nil, encoding.Unknown, fmt.Errorf("decode value tag failed(%v)", err)
	}
	if typ == encoding.Null {
		_, length, err := encoding.PeekValueLength(buf)
		if err != nil {
			return nil, colId, nil, typ, fmt.Errorf("decode null value length failed(%v)", err)
		}
		return buf[length:], colId, nil, typ, nil
	}

	switch typ {
	case encoding.Int:
		remainBuf, ival, err := encoding.DecodeIntValue(buf)
		return remainBuf, colId, hack.Slice(strconv.FormatInt(ival, 10)), typ, err
	case encoding.Float:
		remainBuf, fval, err := encoding.DecodeFloatValue(buf)
		return remainBuf, colId, hack.Slice(strconv.FormatFloat(fval, 'f', -4, 64)), typ, err
	case encoding.Bytes:
		remainBuf, bval, err := encoding.DecodeBytesValue(buf)
		return remainBuf, colId, bval, typ, err
	default:
		return nil, colId, nil, typ, fmt.Errorf("unsupported type(%d) when decoding column(%d)", typ, colId)
	}
}

func encodeAscendingKey(buf []byte, col *basepb.Column, sval []byte) ([]byte, error) {
	switch col.DataType {
	case basepb.DataType_Tinyint, basepb.DataType_Smallint, basepb.DataType_Int, basepb.DataType_BigInt:
		if col.Unsigned {
			ival, err := strconv.ParseUint(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse unsigned integer failed(%s) when encoding pk(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeUvarintAscending(buf, ival), nil
		} else {
			ival, err := strconv.ParseInt(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse integer failed(%s) when encoding pk(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeVarintAscending(buf, ival), nil
		}
	case basepb.DataType_Float, basepb.DataType_Double:
		fval, err := strconv.ParseFloat(hack.String(sval), 64)
		if err != nil {
			return nil, fmt.Errorf("parse float failed(%s) when encoding pk(%s)", err.Error(), col.Name)
		}
		return encoding.EncodeFloatAscending(buf, fval), nil
	case basepb.DataType_Varchar, basepb.DataType_Binary, basepb.DataType_Date, basepb.DataType_TimeStamp:
		return encoding.EncodeBytesAscending(buf, []byte(sval)), nil
	default:
		return nil, fmt.Errorf("unsupported type(%s) when encoding pk(%s)", col.DataType.String(), col.Name)
	}
}

func EncodeSplitKeys(keys []string, columns []*basepb.Column) ([][]byte, error) {
	var ret [][]byte
	for _, c := range columns {
		// Code only according to the first primary key
		if c.GetPrimaryKey() == 1 {
			for _, k := range keys {
				buf, err := EncodePrimaryKey(nil, c, []byte(k))
				if err != nil {
					return nil, err
				}
				ret = append(ret, buf)
			}
			break
		}
	}
	return ret, nil
}

// EncodePrimaryKey encodes the primary key column without encoding the column ID to maintain the sort attribute
func EncodePrimaryKey(buf []byte, col *basepb.Column, sval []byte) ([]byte, error) {
	return encodeAscendingKey(buf, col, sval)
}

// DecodePrimaryKey decodes the primary key column without encoding the column ID to maintain the sort property
func DecodePrimaryKey(buf []byte, col *basepb.Column) ([]byte, []byte, error) {
	switch col.DataType {
	case basepb.DataType_Tinyint, basepb.DataType_Smallint, basepb.DataType_Int, basepb.DataType_BigInt:

		if col.Unsigned {
			buf, v, err := encoding.DecodeUvarintAscending(buf)
			return buf, hack.Slice(strconv.FormatUint(v, 10)), err

		} else {
			buf, v, err := encoding.DecodeVarintAscending(buf)
			return buf, hack.Slice(strconv.FormatInt(v, 10)), err
		}
	case basepb.DataType_Float, basepb.DataType_Double:
		buf, v, err := encoding.DecodeFloatAscending(buf)
		return buf, hack.Slice(strconv.FormatFloat(v, 'f', -4, 64)), err
	case basepb.DataType_Varchar, basepb.DataType_Binary, basepb.DataType_Date, basepb.DataType_TimeStamp:
		ret := make([]byte, 0)
		return encoding.DecodeBytesAscending(buf, ret)
	default:
		return buf, nil, fmt.Errorf("unsupported type(%s) when encoding pk(%s)", col.DataType.String(), col.Name)
	}
}

// DecodePrimaryKey2 encodes primary key columns without encoding column ids to maintain sort properties
func DecodePrimaryKey2(buf []byte, col *basepb.Column) ([]byte, interface{}, error) {
	switch col.DataType {
	case basepb.DataType_Tinyint, basepb.DataType_Smallint, basepb.DataType_Int, basepb.DataType_BigInt:

		if col.Unsigned {
			buf, v, err := encoding.DecodeUvarintAscending(buf)
			return buf, v, err

		} else {
			buf, v, err := encoding.DecodeVarintAscending(buf)
			return buf, v, err
		}
	case basepb.DataType_Float, basepb.DataType_Double:
		buf, v, err := encoding.DecodeFloatAscending(buf)
		return buf, v, err
	case basepb.DataType_Varchar, basepb.DataType_Binary, basepb.DataType_Date, basepb.DataType_TimeStamp:
		ret := make([]byte, 0)
		return encoding.DecodeBytesAscending(buf, ret)
	default:
		return buf, nil, fmt.Errorf("unsupported type(%s) when encoding pk(%s)", col.DataType.String(), col.Name)
	}
}

// Encoding index column ID index column maintains sort properties
func EncodeIndexKey(buf []byte, col *basepb.Column, sval []byte) ([]byte, error) {
	buf = encoding.EncodeUvarintAscending(buf, col.GetId())
	if len(sval) == 0 {
		return encoding.EncodeNullAscending(buf), nil
	}
	return encodeAscendingKey(buf, col, sval)
}
