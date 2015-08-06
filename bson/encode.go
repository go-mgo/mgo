// BSON library for Go
//
// Copyright (c) 2010-2012 - Gustavo Niemeyer <gustavo@niemeyer.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// gobson - BSON library for Go.

package bson

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"time"
	"unicode"

	"gopkg.in/mgo.v2-unstable/decimal"
)

// --------------------------------------------------------------------------
// Some internal infrastructure.

var (
	typeBinary         = reflect.TypeOf(Binary{})
	typeObjectId       = reflect.TypeOf(ObjectId(""))
	typeDBPointer      = reflect.TypeOf(DBPointer{"", ObjectId("")})
	typeSymbol         = reflect.TypeOf(Symbol(""))
	typeMongoTimestamp = reflect.TypeOf(MongoTimestamp(0))
	typeOrderKey       = reflect.TypeOf(MinKey)
	typeDocElem        = reflect.TypeOf(DocElem{})
	typeRawDocElem     = reflect.TypeOf(RawDocElem{})
	typeRaw            = reflect.TypeOf(Raw{})
	typeURL            = reflect.TypeOf(url.URL{})
	typeTime           = reflect.TypeOf(time.Time{})
	typeString         = reflect.TypeOf("")
	typeJSONNumber     = reflect.TypeOf(json.Number(""))
	typeDecimal        = reflect.TypeOf(decimal.Decimal{})
)

const itoaCacheSize = 32

var itoaCache []string

func init() {
	itoaCache = make([]string, itoaCacheSize)
	for i := 0; i != itoaCacheSize; i++ {
		itoaCache[i] = strconv.Itoa(i)
	}
}

func itoa(i int) string {
	if i < itoaCacheSize {
		return itoaCache[i]
	}
	return strconv.Itoa(i)
}

// --------------------------------------------------------------------------
// Marshaling of the document value itself.

type encoder struct {
	out []byte
}

func (e *encoder) addDoc(v reflect.Value) {
	for {
		if vi, ok := v.Interface().(Getter); ok {
			getv, err := vi.GetBSON()
			if err != nil {
				panic(err)
			}
			v = reflect.ValueOf(getv)
			continue
		}
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
			continue
		}
		break
	}

	if v.Type() == typeRaw {
		raw := v.Interface().(Raw)
		if raw.Kind != 0x03 && raw.Kind != 0x00 {
			panic("Attempted to marshal Raw kind " + strconv.Itoa(int(raw.Kind)) + " as a document")
		}
		if len(raw.Data) == 0 {
			panic("Attempted to marshal empty Raw document")
		}
		e.addBytes(raw.Data...)
		return
	}

	start := e.reserveInt32()

	switch v.Kind() {
	case reflect.Map:
		e.addMap(v)
	case reflect.Struct:
		e.addStruct(v)
	case reflect.Array, reflect.Slice:
		e.addSlice(v)
	default:
		panic("Can't marshal " + v.Type().String() + " as a BSON document")
	}

	e.addBytes(0)
	e.setInt32(start, int32(len(e.out)-start))
}

func (e *encoder) addMap(v reflect.Value) {
	for _, k := range v.MapKeys() {
		e.addElem(k.String(), v.MapIndex(k), false)
	}
}

func (e *encoder) addStruct(v reflect.Value) {
	sinfo, err := getStructInfo(v.Type())
	if err != nil {
		panic(err)
	}
	var value reflect.Value
	if sinfo.InlineMap >= 0 {
		m := v.Field(sinfo.InlineMap)
		if m.Len() > 0 {
			for _, k := range m.MapKeys() {
				ks := k.String()
				if _, found := sinfo.FieldsMap[ks]; found {
					panic(fmt.Sprintf("Can't have key %q in inlined map; conflicts with struct field", ks))
				}
				e.addElem(ks, m.MapIndex(k), false)
			}
		}
	}
	for _, info := range sinfo.FieldsList {
		if info.Inline == nil {
			value = v.Field(info.Num)
		} else {
			value = v.FieldByIndex(info.Inline)
		}
		if info.OmitEmpty && isZero(value) {
			continue
		}
		e.addElem(info.Key, value, info.MinSize)
	}
}

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return len(v.String()) == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Slice:
		return v.Len() == 0
	case reflect.Map:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Struct:
		vt := v.Type()
		if vt == typeTime {
			return v.Interface().(time.Time).IsZero()
		}
		for i := 0; i < v.NumField(); i++ {
			if vt.Field(i).PkgPath != "" {
				continue // Private field
			}
			if !isZero(v.Field(i)) {
				return false
			}
		}
		return true
	}
	return false
}

func (e *encoder) addSlice(v reflect.Value) {
	vi := v.Interface()
	if d, ok := vi.(D); ok {
		for _, elem := range d {
			e.addElem(elem.Name, reflect.ValueOf(elem.Value), false)
		}
		return
	}
	if d, ok := vi.(RawD); ok {
		for _, elem := range d {
			e.addElem(elem.Name, reflect.ValueOf(elem.Value), false)
		}
		return
	}
	l := v.Len()
	et := v.Type().Elem()
	if et == typeDocElem {
		for i := 0; i < l; i++ {
			elem := v.Index(i).Interface().(DocElem)
			e.addElem(elem.Name, reflect.ValueOf(elem.Value), false)
		}
		return
	}
	if et == typeRawDocElem {
		for i := 0; i < l; i++ {
			elem := v.Index(i).Interface().(RawDocElem)
			e.addElem(elem.Name, reflect.ValueOf(elem.Value), false)
		}
		return
	}
	for i := 0; i < l; i++ {
		e.addElem(itoa(i), v.Index(i), false)
	}
}

// --------------------------------------------------------------------------
// Marshaling of elements in a document.

func (e *encoder) addElemName(kind byte, name string) {
	e.addBytes(kind)
	e.addBytes([]byte(name)...)
	e.addBytes(0)
}

func (e *encoder) addElem(name string, v reflect.Value, minSize bool) {

	if !v.IsValid() {
		e.addElemName('\x0A', name)
		return
	}

	if getter, ok := v.Interface().(Getter); ok {
		getv, err := getter.GetBSON()
		if err != nil {
			panic(err)
		}
		e.addElem(name, reflect.ValueOf(getv), minSize)
		return
	}

	switch v.Kind() {

	case reflect.Interface:
		e.addElem(name, v.Elem(), minSize)

	case reflect.Ptr:
		e.addElem(name, v.Elem(), minSize)

	case reflect.String:
		s := v.String()
		switch v.Type() {
		case typeObjectId:
			if len(s) != 12 {
				panic("ObjectIDs must be exactly 12 bytes long (got " +
					strconv.Itoa(len(s)) + ")")
			}
			e.addElemName('\x07', name)
			e.addBytes([]byte(s)...)
		case typeSymbol:
			e.addElemName('\x0E', name)
			e.addStr(s)
		case typeJSONNumber:
			n := v.Interface().(json.Number)
			if i, err := n.Int64(); err == nil {
				e.addElemName('\x12', name)
				e.addInt64(i)
			} else if f, err := n.Float64(); err == nil {
				e.addElemName('\x01', name)
				e.addFloat64(f)
			} else {
				panic("failed to convert json.Number to a number: " + s)
			}
		default:
			e.addElemName('\x02', name)
			e.addStr(s)
		}

	case reflect.Float32, reflect.Float64:
		e.addElemName('\x01', name)
		e.addFloat64(v.Float())

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := v.Uint()
		if int64(u) < 0 {
			panic("BSON has no uint64 type, and value is too large to fit correctly in an int64")
		} else if u <= math.MaxInt32 && (minSize || v.Kind() <= reflect.Uint32) {
			e.addElemName('\x10', name)
			e.addInt32(int32(u))
		} else {
			e.addElemName('\x12', name)
			e.addInt64(int64(u))
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v.Type() {
		case typeMongoTimestamp:
			e.addElemName('\x11', name)
			e.addInt64(v.Int())

		case typeOrderKey:
			if v.Int() == int64(MaxKey) {
				e.addElemName('\x7F', name)
			} else {
				e.addElemName('\xFF', name)
			}

		default:
			i := v.Int()
			if (minSize || v.Type().Kind() != reflect.Int64) && i >= math.MinInt32 && i <= math.MaxInt32 {
				// It fits into an int32, encode as such.
				e.addElemName('\x10', name)
				e.addInt32(int32(i))
			} else {
				e.addElemName('\x12', name)
				e.addInt64(i)
			}
		}

	case reflect.Bool:
		e.addElemName('\x08', name)
		if v.Bool() {
			e.addBytes(1)
		} else {
			e.addBytes(0)
		}

	case reflect.Map:
		e.addElemName('\x03', name)
		e.addDoc(v)

	case reflect.Slice:
		vt := v.Type()
		et := vt.Elem()
		if et.Kind() == reflect.Uint8 {
			e.addElemName('\x05', name)
			e.addBinary('\x00', v.Bytes())
		} else if et == typeDocElem || et == typeRawDocElem {
			e.addElemName('\x03', name)
			e.addDoc(v)
		} else {
			e.addElemName('\x04', name)
			e.addDoc(v)
		}

	case reflect.Array:
		et := v.Type().Elem()
		if et.Kind() == reflect.Uint8 {
			e.addElemName('\x05', name)
			if v.CanAddr() {
				e.addBinary('\x00', v.Slice(0, v.Len()).Interface().([]byte))
			} else {
				n := v.Len()
				e.addInt32(int32(n))
				e.addBytes('\x00')
				for i := 0; i < n; i++ {
					el := v.Index(i)
					e.addBytes(byte(el.Uint()))
				}
			}
		} else {
			e.addElemName('\x04', name)
			e.addDoc(v)
		}

	case reflect.Struct:
		switch s := v.Interface().(type) {

		case Raw:
			kind := s.Kind
			if kind == 0x00 {
				kind = 0x03
			}
			if len(s.Data) == 0 && kind != 0x06 && kind != 0x0A && kind != 0xFF && kind != 0x7F {
				panic("Attempted to marshal empty Raw document")
			}
			e.addElemName(kind, name)
			e.addBytes(s.Data...)

		case Binary:
			e.addElemName('\x05', name)
			e.addBinary(s.Kind, s.Data)

		case DBPointer:
			e.addElemName('\x0C', name)
			e.addStr(s.Namespace)
			if len(s.Id) != 12 {
				panic("ObjectIDs must be exactly 12 bytes long (got " +
					strconv.Itoa(len(s.Id)) + ")")
			}
			e.addBytes([]byte(s.Id)...)

		case RegEx:
			e.addElemName('\x0B', name)
			e.addCStr(s.Pattern)
			e.addCStr(s.Options)

		case JavaScript:
			if s.Scope == nil {
				e.addElemName('\x0D', name)
				e.addStr(s.Code)
			} else {
				e.addElemName('\x0F', name)
				start := e.reserveInt32()
				e.addStr(s.Code)
				e.addDoc(reflect.ValueOf(s.Scope))
				e.setInt32(start, int32(len(e.out)-start))
			}

		case time.Time:
			// MongoDB handles timestamps as milliseconds.
			e.addElemName('\x09', name)
			e.addInt64(s.Unix()*1000 + int64(s.Nanosecond()/1e6))

		case url.URL:
			e.addElemName('\x02', name)
			e.addStr(s.String())

		case decimal.Decimal:
			e.addElemName('\x13', name)
			e.addDecimal(s)

		case undefined:
			e.addElemName('\x06', name)

		default:
			e.addElemName('\x03', name)
			e.addDoc(v)
		}

	default:
		panic("Can't marshal " + v.Type().String() + " in a BSON document")
	}
}

// --------------------------------------------------------------------------
// Marshaling of base types.

func (e *encoder) addBinary(subtype byte, v []byte) {
	if subtype == 0x02 {
		// Wonder how that brilliant idea came to life. Obsolete, luckily.
		e.addInt32(int32(len(v) + 4))
		e.addBytes(subtype)
		e.addInt32(int32(len(v)))
	} else {
		e.addInt32(int32(len(v)))
		e.addBytes(subtype)
	}
	e.addBytes(v...)
}

func (e *encoder) addStr(v string) {
	e.addInt32(int32(len(v) + 1))
	e.addCStr(v)
}

func (e *encoder) addCStr(v string) {
	e.addBytes([]byte(v)...)
	e.addBytes(0)
}

func (e *encoder) reserveInt32() (pos int) {
	pos = len(e.out)
	e.addBytes(0, 0, 0, 0)
	return pos
}

func (e *encoder) setInt32(pos int, v int32) {
	e.out[pos+0] = byte(v)
	e.out[pos+1] = byte(v >> 8)
	e.out[pos+2] = byte(v >> 16)
	e.out[pos+3] = byte(v >> 24)
}

func (e *encoder) addInt32(v int32) {
	u := uint32(v)
	e.addBytes(byte(u), byte(u>>8), byte(u>>16), byte(u>>24))
}

func (e *encoder) addInt64(v int64) {
	u := uint64(v)
	e.addBytes(byte(u), byte(u>>8), byte(u>>16), byte(u>>24),
		byte(u>>32), byte(u>>40), byte(u>>48), byte(u>>56))
}

func (e *encoder) addFloat64(v float64) {
	e.addInt64(int64(math.Float64bits(v)))
}

func (e *encoder) addDecimal(v decimal.Decimal) {
	dec128 := DecimalToDec128(v.String())
	e.addBytes(
		byte(dec128[0]),
		byte(dec128[0]>>8),
		byte(dec128[0]>>16),
		byte(dec128[0]>>24),
		byte(dec128[0]>>32),
		byte(dec128[0]>>40),
		byte(dec128[0]>>48),
		byte(dec128[0]>>56),
		byte(dec128[1]),
		byte(dec128[1]>>8),
		byte(dec128[1]>>16),
		byte(dec128[1]>>24),
		byte(dec128[1]>>32),
		byte(dec128[1]>>40),
		byte(dec128[1]>>48),
		byte(dec128[1]>>56))
}

func (e *encoder) addBytes(v ...byte) {
	e.out = append(e.out, v...)
}

// --------------------------------------------------------------------------
// Marshaling helpers.

// multiply64x64 multiplies two uint64's to get a uint128 expressed in 64 bit chunks
func multiply64x64(left uint64, right uint64) [2]uint64 {
	if left == 0 && right == 0 {
		return [2]uint64{0, 0}
	}
	var leftHigh = left >> 32
	var leftLow = uint64(uint32(left))
	var rightHigh = right >> 32
	var rightLow = uint64(uint32(right))

	var productHigh = leftHigh * rightHigh
	var productMidl = leftHigh * rightLow
	var productMidr = leftLow * rightHigh
	var productLow = leftLow * rightLow

	productHigh += (productMidl >> 32)
	productMidl = uint64(uint32(productMidl)) + productMidr + (productLow >> 32)

	productHigh += (productMidl >> 32)
	productLow = (productMidl << 32) + uint64(uint32(productLow))

	return [2]uint64{productLow, productHigh}
}

// makeDec128Infinity returns a Dec128 +/-Infinity depending on the parameter
func makeDec128Infinity(negative bool) [2]uint64 {
	if !negative {
		return [2]uint64{0, 0x7800000000000000}
	}
	return [2]uint64{0, 0xf800000000000000}
}

// makeDec128NaN returns a Dec128 NaN
func makeDec128NaN() [2]uint64 {
	return [2]uint64{0, 0x7c00000000000000}
}

// DecimalToDec128 converts a decimal string to two uint64s
func DecimalToDec128(str string) [2]uint64 {
	// State tracking
	var isNegative = false
	var sawRadix = false
	var foundNonZero = false

	var nDigitsNoTrailing uint16 // Total number of sig. digits (no trailing zeros)
	var nDigitsRead uint16	  	 // Total number of significand digits read
	var nDigits uint16		  	 // Total number of sig. digits
	var radixPosition uint16  	 // The number of digits after the radix point
	var firstNonZero uint16	  	 // The index of the first non zero

	const BSONDecimalMaxDigits int = 34
	const BSONDecimalExponentMax int = 6111
	const BSONDecimalExponentBias int = 6176
	const BSONDecimalExponentMin int = -6176
	var digits [BSONDecimalMaxDigits]uint16
	var digitsInsertPosition int
	var nDigitsStored uint16
	var firstDigit uint16		 // The index of the first digit
	var lastDigit uint16		 // The index of the last digit

	var exponent int32

	var strRead = 0
	var strLen = len(str)

	// Check sign
	if str[strRead] == '-' {
		isNegative = true
		strRead++
	}

	// Check for Infinity
	if str[strRead:] == "Infinity" {
		return makeDec128Infinity(isNegative)
	}

	// Check for NaN
	if str[strRead:] == "NaN" {
		return makeDec128NaN()
	}

	// Read digits
	for (unicode.IsDigit(rune(str[strRead])) || str[strRead] == '.') {
		if str[strRead] == '.' {
			sawRadix = true
			if strRead < strLen - 1 {
				strRead++
			} else {
				break
			}
			continue
		}

		if nDigitsStored < 34 {
			if str[strRead] != '0' || foundNonZero {
				if !foundNonZero {
					firstNonZero = nDigitsRead
				}
				foundNonZero = true
				digits[digitsInsertPosition] = uint16(str[strRead]) - '0'
				digitsInsertPosition++
				nDigitsStored++
			}
		}

		if foundNonZero {
			nDigits++
		}

		if sawRadix {
			radixPosition++
		}

		nDigitsRead++
		if strRead < strLen - 1 {
			strRead++
		} else {
			break
		}
	}

	// Done reading input
	// Find first non-zero digit in digits
	if nDigitsStored == 0 {
		firstDigit = 0
		lastDigit = 0
		digits[0] = 0
		nDigits = 1
		nDigitsStored = 1
		nDigitsNoTrailing = 0
	} else {
		lastDigit = nDigitsStored - 1
		nDigitsNoTrailing = nDigits
		for str[firstNonZero + nDigitsNoTrailing - 1] == '0' {
			nDigitsNoTrailing--;
		}
	}

	// Normalization of exponent
	// Correct exponent based on radix position and shift significand as needed
	// to represent user input.

	// Overflow prevention
	if exponent <= int32(radixPosition) && int32(radixPosition) - exponent > (1 << 14) {
		exponent = int32(BSONDecimalExponentMin)
	} else {
		exponent -= int32(radixPosition)
	}

	// Attempt to normalize the exponent
	for exponent > int32(BSONDecimalExponentMax) {
		// Shift the exponent to significand and decrease
		lastDigit++

		if lastDigit - firstDigit > uint16(BSONDecimalMaxDigits) {
			return makeDec128Infinity(isNegative)
		}

		exponent--
	}

	for exponent < int32(BSONDecimalExponentMin) || nDigitsStored < nDigits {
		// Shift the last digit
		if lastDigit == 0 {
			exponent = int32(BSONDecimalExponentMin)
			// Signal zero value
			nDigitsNoTrailing = 0
			break;
		}

		if nDigitsStored < nDigits {
			// Adjust to match digits not stored
			nDigits--
		} else {
			// Adjust to round
			lastDigit--
		}

		if exponent < int32(BSONDecimalExponentMax) {
			exponent++
		} else {
			return makeDec128Infinity(isNegative)
		}
	}

	// Round
	if lastDigit - firstDigit + 1 < nDigitsNoTrailing {
		var endOfString = nDigitsRead
		// If we have seen a radix point, str is 1 longer than we have documented
		// so inc the position of the first nonzero and the position digits are read to
		if (sawRadix && exponent <= int32(BSONDecimalExponentMin)) {
			firstNonZero++
			endOfString++
		}
		// There are non-zero digits after lastDigit that need rounding
		// Use round to nearest, ties to even rounding mode
		var roundDigit = str[firstNonZero + lastDigit + 1] - '0'
		var roundBit = false
		if roundDigit >= 5 {
			roundBit = true

			if roundDigit == 5 {
				roundBit = (digits[lastDigit] % 2 == 1)

				for i := firstNonZero + lastDigit + 2; i < endOfString; i++ {
					if (str[i] - '0') != 0 {
						roundBit = true
						break
					}
				}
			}
		}

		if roundBit {
			var dIdx = lastDigit
			for ; dIdx >= 0; dIdx-- {
				digits[dIdx]++
				if digits[dIdx] > 9 {
					digits[dIdx] = 0
					// Overflowed most significant digit
					if dIdx == 0 {
						if exponent < int32(BSONDecimalExponentMax) {
							exponent++
							digits[dIdx] = 1
							break
						} else {
							return makeDec128Infinity(isNegative)
						}
					}
				} else {
					break
				}
			}
		}
	}

	// Encode significand
	var significandHigh uint64
	var significandLow uint64

	if nDigitsNoTrailing == 0 {
		// If we read a zero, significand is zero
		significandHigh = 0
		significandLow = 0
	} else if lastDigit - firstDigit < 17 {
		// If we can fit the significand entirely into the low 64 bits
		var dIdx = firstDigit
		significandLow = uint64(digits[dIdx])
		dIdx++

		for ; dIdx <= lastDigit; dIdx++ {
			significandLow *= 10
			significandLow += uint64(digits[dIdx])
			significandHigh = 0
		}
	} else {
		// We need to use both the low 64 and high 64 bits
		var dIdx = firstDigit
		significandHigh = uint64(digits[dIdx])
		dIdx++

		for ; dIdx <= lastDigit - 17; dIdx++ {
			significandHigh *= 10
			significandHigh += uint64(digits[dIdx])
		}

		significandLow = uint64(digits[dIdx])
		dIdx++
		for ; dIdx <= lastDigit; dIdx++ {
			significandLow *= 10
			significandLow += uint64(digits[dIdx])
		}
	}

	significand := multiply64x64(significandHigh, 100000000000000000)
	significand[0] += significandLow
	if significand[0] < significandLow {
		significand[1]++
	}

	// Encode combination and exponent with significand
	var biasedExponent = uint16(exponent + int32(BSONDecimalExponentBias))
	var decHigh64 uint64
	var decLow64 uint64

	if (significand[1] >> 49) & 1 != 0 {
		decHigh64 |= (0x3 << 61)
		decHigh64 |= uint64((biasedExponent & 0x3fff)) << 47
		decHigh64 |= (significand[1] & 0x7fffffffffff)
	} else {
		decHigh64 |= uint64((biasedExponent & 0x3fff)) << 49
		decHigh64 |= (significand[1] & 0x1ffffffffffff)
	}

	decLow64 = significand[0]

	// Encode sign
	if isNegative {
		decHigh64 |= 0x8000000000000000
	}

	return [2]uint64{decLow64, decHigh64}
}
