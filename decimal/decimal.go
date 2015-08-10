// Package decimal implements support for a decimal data type
package decimal

import (
	"errors"
	"math/big"
	"unicode"
)

// A Decimal type for the mgo driver
type Decimal struct {
	finite bool
	// Post decimal point precision
	precision int
	rational big.Rat
}

// String takes an integer that signifies the precision (post decimal point) at which
// to print a decimal and it returns that string representation
func (d Decimal) String(prec int) string {
	if !d.finite {
		if d.rational.Sign() == 1 {
			return "Infinity"
		} else if d.rational.Sign() == -1 {
			return "-Infinity"	
		} else {
			return "NaN"
		}
	}
	return d.rational.FloatString(prec)
}

// Parse takes a string and constructs a Decimal from the string
func Parse(s string) (Decimal, error) {
	precision := calculatePrecision(s)
	rat := new(big.Rat)
	_, success := rat.SetString(s)
	if success {
		return Decimal{true, precision, *rat}, nil
	} else if s == "Inf" || s == "Infinity" {
		// Return a decimal of 1 but note not finite
		return Decimal{false, 0, *big.NewRat(1, 1)}, nil
	} else if s == "-Inf" || s == "-Infinity" {
		// Return a decimal of -1 but note not finite
		return Decimal{false, 0, *big.NewRat(-1, 1)}, nil
	}
	// Return Decimal of 0 but note note finite and error
	return Decimal{false, 0, *big.NewRat(0, 1)}, errors.New("Cannot create Decimal '" + s + "'")
}

// calculatePrecision takes a decimal string and returns its precision (post decimal point)
func calculatePrecision(s string) int {
	var prec = 0
	var sawRadix = false
	var sawExponent = false
	for _, c := range s {
		if c == '.' {
			sawRadix = true
		} else if c == 'e' || c == 'E' {
			sawExponent = true
		}
		if sawRadix && !sawExponent && unicode.IsDigit(rune(c)) {
			prec++
		}
	}
	if prec == 0 && sawExponent {
		// TODO: Investigate whether to log a note here because creating a decimal
		// such as 2E-50 from string will maintain 34 digits of precision (post decimal)
		// and actually be equal to 0
		prec = 34
	}
	return prec
}

// GetPrecision returns the stored precision of a Decimal
func (d Decimal) GetPrecision() int {
	return d.precision
}