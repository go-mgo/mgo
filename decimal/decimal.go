// Package decimal implements support for a decimal data type
package decimal

import (
	"errors"
	"math/big"
)

// A Decimal type for the mgo driver
type Decimal struct {
	finite bool
	rational big.Rat
}

// String returns a float string representation of a Decimal
func (d Decimal) String() string {
	// Retrieve a copy of the Decimal's internal big.Rat denominator
	denom := new(big.Int)
	denom.Set(d.rational.Denom())
	// Discover the precision of the denominator and use it to fix
	// the precision of the string conversion
	var precision = 0
	one := big.NewInt(1)
	ten := big.NewInt(10)
	for denom.Cmp(one) > 0 {
		denom = denom.Div(denom, ten)
		precision++
	}

	if !d.finite {
		if d.rational.Sign() == 1 {
			return "Infinity"
		} else if d.rational.Sign() == -1 {
			return "-Infinity"	
		} else {
			return "NaN"
		}
	}

	return d.rational.FloatString(precision)
}

// Parse takes a string and constructs a Decimal from the string
func Parse(s string) (Decimal, error) {
	rat := new(big.Rat)
	_, success := rat.SetString(s)
	if success {
		return Decimal{true, *rat}, nil
	} else if s == "Inf" || s == "Infinity" {
		// Return a decimal of 1 but note not finite
		return Decimal{false, *big.NewRat(1, 1)}, nil
	} else if s == "-Inf" || s == "-Infinity" {
		// Return a decimal of -1 but note not finite
		return Decimal{false, *big.NewRat(-1, 1)}, nil
	}
	// Return Decimal of 0 but note note finite and error
	return Decimal{false, *big.NewRat(0, 1)}, errors.New("Cannot create Decimal '" + s + "'")
}
