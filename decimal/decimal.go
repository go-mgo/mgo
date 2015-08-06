// Package decimal implements support for a decimal data type
package decimal

import (
	"strconv"
	"strings"
	"unicode"
)

// --------------------------------------------------------------------------
// Internal Helpers for Decimal

// uint128_32 is a struct that represents a 128 bit integer and is used
// internally for ToString and MakeDecimal methods for the Decimal type.
type uint128_32 struct {
	// Stored as four 32 bit words high to low
	parts [4]uint32
}

// This function divides a uint128_32 by 1000000000 (1 billion) and
// computes the quotient and remainder
func (value uint128_32) Divide1B() (uint128_32, uint32) {
	var quotient uint128_32
	var remainder uint64
	var divisor uint32 = 1000 * 1000 * 1000

	if value.parts[0] == 0 && value.parts[1] == 0 &&
	   value.parts[2] == 0 && value.parts[3] == 0 {
		quotient = value
		return quotient, uint32(remainder)
	}

	for i := 0; i <= 3; i++ {
		// Adjust remainder to match value of next dividend
		remainder <<= 32
		// Add the dividend to remainder
		remainder += uint64(value.parts[i])
		quotient.parts[i] = uint32(remainder / uint64(divisor))
		remainder %= uint64(divisor)
	}

	return quotient, uint32(remainder)
}

// uint128_64 is a struct that represents a 128 bit integer and is used
// internally for the Parse method for the Decimal type.
type uint128_64 struct {
	High uint64
	Low uint64
}

// multiply64x64 multiplies two uint64's to get a uint128_64
func multiply64x64(left uint64, right uint64) uint128_64 {
	if left == 0 && right == 0 {
		return uint128_64{0, 0}
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

	return uint128_64{productHigh, productLow}
}

// --------------------------------------------------------------------------
// Decimal type struct and member methods

// Decimal refers to the 128-bit IEEE 754-2008 decimal type.
type Decimal struct {
	Low64  uint64
	High64 uint64
}

// IsNaN returns a bool signaling whether the caller equals NaN
func (dec Decimal) IsNaN() bool {
	return dec.High64 == 0x7c00000000000000
}

// IsPositiveInfinity returns a bool signaling whether the calling Decimal
// equals +infinity
func (dec Decimal) IsPositiveInfinity() bool {
	return dec.High64 == 0x7800000000000000
}

// IsNegativeInfinity returns a bool signaling whether the calling Decimal
// equals -infinity
func (dec Decimal) IsNegativeInfinity() bool {
	return dec.High64 == 0xf800000000000000
}

// IsIdentical returns a bool signaling whether the calling Deciaml
// is identical to the passed in Decimal
func (dec Decimal) IsIdentical(other Decimal) bool {
	return (dec.High64 == other.High64) && (dec.Low64 == other.Low64)
}

func (dec Decimal) String() string {
	var combinationMask uint32 = 0x1f    // Extract least significant 5 bits
	var exponentMask uint32 = 0x3fff     // Extract least significant 14 bits
	var combinationInfinity uint32 = 30  // Value of combination field for Inf
	var combinationNaN uint32 = 31		 // Value of combination field for NaN
	var exponentBias uint32 = 6176       // decimal128 exponent bias

	var outStr string

	var low = uint32(dec.Low64)		     // Bits 0 - 31
	var midl = uint32(dec.Low64 >> 32)   // Bits 32 - 63
	var midh = uint32(dec.High64)        // Bits 64 - 95
	var high = uint32(dec.High64 >> 32)  // Bits 96 - 107
	var combination uint32               // Bits 1 - 5
	var biasedExponent uint32            // Decoded 14 bit biased exponent
	var significandDigits uint32         // Number of significand digits
	var significand [36]uint32           // Base 10 digits in significand
	var exponent int32	                 // Unbiased exponent
	var isZero bool 	                 // True if the number is zero
	var significandMSB uint              // Most significant bits (50 - 46)

	// dec is negative
	if int64(dec.High64) < 0 {
		outStr += "-"
	}

	// Decode combination field and exponent
	combination = (high >> 26) & combinationMask

	if (combination >> 3) == 3 {
		// Check for special values
		if combination == combinationInfinity {
			outStr += "Infinity"
			return outStr
		} else if combination == combinationNaN {
			// Drop the sign, +NaN and -NaN behave the same in MongoDB
			outStr = "NaN"
			return outStr
		} else {
			biasedExponent = (high >> 15) & exponentMask
			significandMSB = uint(0x8 + (high >> 14) & 0x01)
		}
	} else {
		biasedExponent = (high >> 17) & exponentMask
		significandMSB = uint(high >> 14) & 0x7
	}

	exponent = int32(biasedExponent - exponentBias)

	// Convert 114 bit binary number in the significand to at most
	// 34 decimal digits using modulo and division.
	var significand128 = uint128_32{[4]uint32{
		(high & exponentMask) + (uint32(significandMSB & 0xf) << 14),
		midh,
		midl,
		low,
	}}

	if significand128.parts[0] == 0 && significand128.parts[1] == 0 &&
	   significand128.parts[2] == 0 && significand128.parts[3] == 0 {
	   	isZero = true
	} else {
		var leastDigits uint32
		for k := 3; k >= 0; k-- {
			significand128, leastDigits = significand128.Divide1B()

			// We now have the 9 least significand digits (in base 2).
			// Convert and output to a string
			if leastDigits == 0 {
				continue
			}

			for j := 8; j >= 0; j-- {
				significand[k * 9 + j] = leastDigits % 10
				leastDigits /= 10
			}
		}
	}

	// Output format options
	// Scientific : [-]d.dddE(+/-)dd or [-]dE(+/-)dd
	// Regular    : ddd.ddd
	var significandRead uint32
	if isZero {
		significandDigits = 1
		significandRead = 0
	} else {
		significandDigits = 36
		// Move significandRead to where the significand is not led with zeros
		for significandRead < 35 && significand[significandRead] == 0 {
			significandDigits--
			significandRead++
		}
	}
	var scientificExponent = int32(significandDigits) - 1 + exponent

	if scientificExponent >= 12 || scientificExponent <= -4 ||
	   exponent > 0 || (isZero && scientificExponent != 0) {
		// Scientific format
		outStr += string(significand[significandRead] + '0')
		significandRead++
		significandDigits--

		if significandDigits != 0 {
			outStr += "."
		}

		for i := uint32(0); i < significandDigits; i++ {
			outStr += string(significand[significandRead] + '0')
			significandRead++
		}
		// Exponent
		outStr += "E"
		if scientificExponent > 0 {
			outStr += "+"
		}
		outStr += strconv.Itoa(int(scientificExponent))
	} else {
		// Regular format
		if exponent >= 0 {
			for i := uint32(0); i < significandDigits; i++ {
				outStr += string(significand[significandRead] + '0')
				significandRead++
			}
		} else {
			var radixPosition = int32(significandDigits) + exponent
			if radixPosition > 0 {
				// If we have non-zero digits before the radix
				for i := int32(0); i< radixPosition; i++ {
					outStr += string(significand[significandRead] + '0')
					significandRead++
				}
			} else {
				// Add a leading zero before radix point
				outStr += "0"
			}

			outStr += "."
			for radixPosition < 0 {
				// Add leading zeros after radix point
				outStr += "0"
				radixPosition++
			}
			var maxRadixPosition uint32
			if radixPosition - 1 > 0 {
				maxRadixPosition = uint32(radixPosition - 1)
			}
			for i := uint32(0); i < significandDigits - maxRadixPosition; i++ {
				outStr += string(significand[significandRead] + '0')
				if significandRead >= uint32(len(significand) - 1) {
					break
				}
				significandRead++
			}
		}
	}

	return outStr
}

// --------------------------------------------------------------------------
// Decimal non-member methods

// NewDecimalInfinity returns a Decimal +/-Infinity depending on the parameter
func NewDecimalInfinity(negative bool) Decimal {
	if !negative {
		return Decimal{0, 0x7800000000000000}
	}
	return Decimal{0, 0xf800000000000000}
}

// NewDecimalNaN returns a Decimal NaN
func NewDecimalNaN() Decimal {
	return Decimal{0, 0x7c00000000000000}
}

// Parse converts a string in the format [+/-]ddd[.]ddd[E][+/-]dddd
// to a Decimal type. Out of range values are converted to +/-Infinity
// and invalid strings are converted to NaN.
func Parse(str string) Decimal {
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

	// Strip off whitespace
	str = strings.TrimSpace(str)

	// Check for empty string
	if strLen == 0 {
		return NewDecimalNaN()
	}

	// Check sign
	if str[strRead] == '+' || str[strRead] == '-' {
		isNegative = (str[strRead] == '-')
		strRead++
	}

	if (strRead >= strLen) {
		return NewDecimalNaN()
	}

	// Check for Infinity and NaN
	if unicode.IsLetter(rune(str[strRead])) || !unicode.IsDigit(rune(str[strRead])) ||
	   str[strRead] == '.' {
		if str[strRead] == 'i' || str[strRead] == 'I' {
			strRead++
			if strRead >= strLen {
				return NewDecimalNaN()
			}
			if str[strRead] == 'n' || str[strRead] == 'N' {
				strRead++
				if strRead >= strLen {
					return NewDecimalNaN()
				}
				if str[strRead] == 'f' || str[strRead] == 'F' {
					return NewDecimalInfinity(isNegative)
				}
			}
		}

		return NewDecimalNaN()
	}

	// Read digits
	for (unicode.IsDigit(rune(str[strRead])) || str[strRead] == '.') {
		if str[strRead] == '.' {
			if sawRadix {
				return NewDecimalNaN()
			}

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

	if sawRadix && nDigitsRead == 0 {
		return NewDecimalNaN()
	}

	// Read exponent if it exists
	if str[strRead] == 'e' || str[strRead] == 'E' {
		// var nRead int = 0
		strRead++
		readExponent, err := strconv.Atoi(string(str[strRead:]))
		exponent = int32(readExponent)
		strRead = strLen - 1

		if err != nil {
			return NewDecimalNaN()
		}
	}

	if strRead != strLen - 1 {
		return NewDecimalNaN()
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
			return NewDecimalInfinity(isNegative)
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
			return NewDecimalInfinity(isNegative)
		}
	}

	// Round
	if lastDigit - firstDigit + 1 < nDigitsNoTrailing {
		// There are non-zero digits after lastDigit that need rounding
		// Use round to nearest, ties to even rounding mode
		var roundDigit = str[firstNonZero + lastDigit + 1] - '0'
		var roundBit = false

		if roundDigit >= 5 {
			roundBit = true

			if roundDigit == 5 {
				roundBit = (digits[lastDigit] % 2 == 1)

				for i := firstNonZero + lastDigit + 2; i < nDigitsRead; i++ {
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
							return NewDecimalInfinity(isNegative)
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
	significand.Low += significandLow
	if significand.Low < significandLow {
		significand.High++
	}

	// Encode combination and exponent with significand
	var biasedExponent = uint16(exponent + int32(BSONDecimalExponentBias))
	var decHigh64 uint64
	var decLow64 uint64

	if (significand.High >> 49) & 1 != 0 {
		decHigh64 |= (0x3 << 61)
		decHigh64 |= uint64((biasedExponent & 0x3fff)) << 47
		decHigh64 |= (significand.High & 0x7fffffffffff)
	} else {
		decHigh64 |= uint64((biasedExponent & 0x3fff)) << 49
		decHigh64 |= (significand.High & 0x1ffffffffffff)
	}

	decLow64 = significand.Low

	// Encode sign
	if isNegative {
		decHigh64 |= 0x8000000000000000
	}

	result := Decimal{decLow64, decHigh64}
	return result
}
