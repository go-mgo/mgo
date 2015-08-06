package decimal_test

import (
	"testing"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2-unstable/decimal"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

type S struct{}

var _ = Suite(&S{})

// --------------------------------------------------------------------------
// Tests for the Decimal type.

func (s *S) TestDecimalIsNaN(c *C) {
	NaN := decimal.Decimal{0, 0x7c00000000000000}
	notNaN := decimal.Decimal{0, 0}
	NaNConstructed := decimal.NewDecimalNaN()
	c.Assert(NaN.IsNaN(), Equals, true)
	c.Assert(notNaN.IsNaN(), Equals, false)
	c.Assert(NaNConstructed.IsNaN(), Equals, true)
}

func (s *S) TestDecimalIsPositiveInfinity(c *C) {
	positiveInfinity := decimal.Decimal{0, 0x7800000000000000}
	positiveInfinityConstructed := decimal.NewDecimalInfinity(false)
	negativeInfinity := decimal.Decimal{0, 0xf800000000000000}
	c.Assert(positiveInfinity.IsPositiveInfinity(), Equals, true)
	c.Assert(positiveInfinityConstructed.IsPositiveInfinity(), Equals, true)
	c.Assert(negativeInfinity.IsPositiveInfinity(), Equals, false)
}

func (s *S) TestDecimalIsNegativeInfinity(c *C) {
	negativeInfinity := decimal.Decimal{0, 0xf800000000000000}
	negativeInfinityConstructed := decimal.NewDecimalInfinity(true)
	positiveInfinity := decimal.Decimal{0, 0x7800000000000000}
	c.Assert(negativeInfinity.IsNegativeInfinity(), Equals, true)
	c.Assert(negativeInfinityConstructed.IsNegativeInfinity(), Equals, true)
	c.Assert(positiveInfinity.IsNegativeInfinity(), Equals, false)
}

func (s *S) TestDecimalIsIdentical(c *C) {
	one := decimal.Decimal{0x0000000000000001, 0x3040000000000000}
	two  := decimal.Decimal{0x0000000000000002, 0x3040000000000000}
	c.Assert(one.IsIdentical(one), Equals, true)
	c.Assert(two.IsIdentical(two), Equals, true)
	c.Assert(one.IsIdentical(two), Equals, false)
	c.Assert(two.IsIdentical(one), Equals, false)
}

func (s *S) TestDecimalStringInfinity(c *C) {
	positiveInfinity := decimal.Decimal{0, 0x7800000000000000}
	negativeInfinity := decimal.Decimal{0, 0xf800000000000000}
	c.Assert(positiveInfinity.String(), Equals, "Infinity")
	c.Assert(negativeInfinity.String(), Equals, "-Infinity")
}

func (s *S) TestDecimalStringNaN(c *C) {
	pNaN := decimal.Decimal{0, 0x7c00000000000000}
	nNaN := decimal.Decimal{0, 0xfc00000000000000}
	psNaN := decimal.Decimal{0, 0x7e00000000000000}
	nsNaN := decimal.Decimal{0, 0xfe00000000000000}
	payloadNaN := decimal.Decimal{12, 0x7e00000000000000}
	c.Assert(pNaN.String(), Equals, "NaN")
	c.Assert(nNaN.String(), Equals, "NaN")
	c.Assert(psNaN.String(), Equals, "NaN")
	c.Assert(nsNaN.String(), Equals, "NaN")
	c.Assert(payloadNaN.String(), Equals, "NaN")
}

func (s *S) TestDecimalStringRegular(c *C) {
	one := decimal.Decimal{0x0000000000000001, 0x3040000000000000}
	// 10E-1
	oneShift := decimal.Decimal{0x000000000000000a, 0x303e000000000000}
	zero := decimal.Decimal{0x0000000000000000, 0x3040000000000000}
	two  := decimal.Decimal{0x0000000000000002, 0x3040000000000000}
	negativeOne := decimal.Decimal{0x0000000000000001, 0xb040000000000000}
	negativeZero := decimal.Decimal{0x0000000000000000, 0xb040000000000000}
	// 0.1
	tenth := decimal.Decimal{0x0000000000000001, 0x303e000000000000}
	// 0.001234
	smallestRegular := decimal.Decimal{0x00000000000004d2, 0x3034000000000000}
	// 12345789012
	largestRegular  := decimal.Decimal{0x0000001cbe991a14, 0x3040000000000000}
	// 0.00123400000
	trailingZeros := decimal.Decimal{0x00000000075aef40, 0x302a000000000000}
	// 0.1234567890123456789012345678901234
	allDigits := decimal.Decimal{0xde825cd07e96aff2, 0x2ffc3cde6fff9732}
	c.Assert(one.String(), Equals, "1")
	c.Assert(oneShift.String(), Equals, "1.0")
	c.Assert(zero.String(), Equals, "0")
	c.Assert(two .String(), Equals, "2")
	c.Assert(negativeOne.String(), Equals, "-1")
	c.Assert(negativeZero.String(), Equals, "-0")
	c.Assert(tenth.String(), Equals, "0.1")
	c.Assert(smallestRegular.String(), Equals, "0.001234")
	c.Assert(largestRegular .String(), Equals, "123456789012")
	c.Assert(trailingZeros.String(), Equals, "0.00123400000")
	c.Assert(allDigits.String(), Equals, "0.1234567890123456789012345678901234")
}

func (s *S) TestDecimalStringScientific(c *C) {
	// 1.000000000000000000000000000000000E+6144
	huge := decimal.Decimal{0x38c15b0a00000000, 0x5ffe314dc6448d93}
	// 1E-6176
	tiny := decimal.Decimal{0x0000000000000001, 0x0000000000000000}
	// -1E-6176i
	negTiny := decimal.Decimal{0x0000000000000001, 0x8000000000000000}
	// 9.999987654321E+112
	large := decimal.Decimal{0x000009184db63eb1, 0x3108000000000000}
	// 9.999999999999999999999999999999999E+6144
	largest := decimal.Decimal{0x378d8e63ffffffff, 0x5fffed09bead87c0}
	// 9.999999999999999999999999999999999E-6143
	tiniest := decimal.Decimal{0x378d8e63ffffffff, 0x0001ed09bead87c0}
	// 5.192296858534827628530496329220095E+33
	fullHouse := decimal.Decimal{0xffffffffffffffff, 0x3040ffffffffffff}
	c.Assert(huge.String(), Equals, "1.000000000000000000000000000000000E+6144")
	c.Assert(tiny.String(), Equals, "1E-6176")
	c.Assert(negTiny.String(), Equals, "-1E-6176")
	c.Assert(large.String(), Equals, "9.999987654321E+112")
	c.Assert(largest.String(), Equals, "9.999999999999999999999999999999999E+6144")
	c.Assert(tiniest.String(), Equals, "9.999999999999999999999999999999999E-6143")
	c.Assert(fullHouse.String(), Equals, "5.192296858534827628530496329220095E+33")
}

func (s *S) TestDecimalStringZeros(c *C) {
	// 0
	zero := decimal.Decimal{0x0000000000000000, 0x3040000000000000};
	// 0E+300
	posExpZero := decimal.Decimal{0x0000000000000000, 0x3298000000000000};
	// 0E-600
	negExpZero := decimal.Decimal{0x0000000000000000, 0x2b90000000000000}
	c.Assert(zero.String(), Equals, "0")
	c.Assert(posExpZero.String(), Equals, "0E+300")
	c.Assert(negExpZero.String(), Equals, "0E-600")
}

func (s *S) TestDecimalFromStringInvalidInputs(c *C) {
	c.Assert(decimal.Parse(".").IsNaN(), Equals, true)
	c.Assert(decimal.Parse(".e").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("invalid").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("in").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("i").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("E02").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("..1").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("1abcede").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("1.24abc").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("1.24abcE+02").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("1.24E+02abc2d").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("E+02").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("e+02").IsNaN(), Equals, true)
}

func (s *S) TestDecimalFromStringInfinity(c *C) {
	c.Assert(decimal.Parse("Infinity").IsPositiveInfinity(), Equals, true)
	c.Assert(decimal.Parse("+Infinity").IsPositiveInfinity(), Equals, true)
	c.Assert(decimal.Parse("+Inf").IsPositiveInfinity(), Equals, true)
	c.Assert(decimal.Parse("-Inf").IsNegativeInfinity(), Equals, true)
	c.Assert(decimal.Parse("-Infinity").IsNegativeInfinity(), Equals, true)
}

func (s *S) TestDecimalFromStringNaN(c *C) {
	c.Assert(decimal.Parse("NaN").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("+NaN").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("-NaN").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("-nan").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("1e").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("+nan").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("nan").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("Nan").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("+Nan").IsNaN(), Equals, true)
	c.Assert(decimal.Parse("-Nan").IsNaN(), Equals, true)
}

func (s *S) TestDecimalFromStringSimple(c *C) {
	one := decimal.Parse("1");
	negativeOne := decimal.Parse("-1");
	zero := decimal.Parse("0");
	negativeZero := decimal.Parse("-0");
	number := decimal.Parse("12345678901234567");
	numberTwo := decimal.Parse("989898983458");
	negativeNumber := decimal.Parse("-12345678901234567");
	fractionalNumber := decimal.Parse("0.12345");
	leadingZero := decimal.Parse("0.0012345");
	leadingInsignificantZeros := decimal.Parse("00012345678901234567");
	c.Assert(one.IsIdentical(decimal.Decimal{0x0000000000000001, 0x3040000000000000}), Equals, true);
	c.Assert(negativeOne.IsIdentical(decimal.Decimal{0x0000000000000001, 0xb040000000000000}), Equals, true);
	c.Assert(zero.IsIdentical(decimal.Decimal{0x0000000000000000, 0x3040000000000000}), Equals, true);
	c.Assert(negativeZero.IsIdentical(decimal.Decimal{0x0000000000000000, 0xb040000000000000}), Equals, true);
	c.Assert(number.IsIdentical(decimal.Decimal{0x002bdc545d6b4b87, 0x3040000000000000}), Equals, true);
	c.Assert(numberTwo.IsIdentical(decimal.Decimal{0x000000e67a93c822, 0x3040000000000000}), Equals, true);
	c.Assert(negativeNumber.IsIdentical(decimal.Decimal{0x002bdc545d6b4b87, 0xb040000000000000}), Equals, true);
	c.Assert(fractionalNumber.IsIdentical(decimal.Decimal{0x0000000000003039, 0x3036000000000000}), Equals, true);
	c.Assert(leadingZero.IsIdentical(decimal.Decimal{0x0000000000003039, 0x3032000000000000}), Equals, true);
	c.Assert(leadingInsignificantZeros.IsIdentical(decimal.Decimal{0x002bdc545d6b4b87, 0x3040000000000000}), Equals, true);
}

func (s *S) TestDecimalFromStringScientific(c *C) {
	ten := decimal.Parse("10e0");
	tenAgain := decimal.Parse("1e1");
	one := decimal.Parse("10e-1");
	hugeExp := decimal.Parse("12345678901234567e6111");
	tinyExp := decimal.Parse("1e-6176");
	fractional := decimal.Parse("-100E-10");
	c.Assert(ten.IsIdentical(decimal.Decimal{0x000000000000000a, 0x3040000000000000}), Equals, true)
	c.Assert(tenAgain.IsIdentical(decimal.Decimal{0x0000000000000001, 0x3042000000000000}), Equals, true)
	c.Assert(one.IsIdentical(decimal.Decimal{0x000000000000000a, 0x303e000000000000}), Equals, true)
	c.Assert(hugeExp.IsIdentical(decimal.Decimal{0x002bdc545d6b4b87, 0x5ffe000000000000}), Equals, true)
	c.Assert(tinyExp.IsIdentical(decimal.Decimal{0x0000000000000001, 0x0000000000000000}), Equals, true)
	c.Assert(fractional.IsIdentical(decimal.Decimal{0x0000000000000064, 0xb02c000000000000}), Equals, true)
}

func (s *S) TestDecimalFromStringLarge(c *C) {
	large := decimal.Parse("12345689012345789012345");
	allDigits := decimal.Parse("1234567890123456789012345678901234");
	largest := decimal.Parse("9.999999999999999999999999999999999E+6144");
	tiniest := decimal.Parse("9.999999999999999999999999999999999E-6143");
	fullHouse := decimal.Parse("5.192296858534827628530496329220095E+33");

	c.Assert(large.IsIdentical(decimal.Decimal{0x42da3a76f9e0d979, 0x304000000000029d}), Equals, true)
	c.Assert(allDigits.IsIdentical(decimal.Decimal{0xde825cd07e96aff2, 0x30403cde6fff9732}), Equals, true)
	c.Assert(largest.IsIdentical(decimal.Decimal{0x378d8e63ffffffff, 0x5fffed09bead87c0}), Equals, true)
	c.Assert(tiniest.IsIdentical(decimal.Decimal{0x378d8e63ffffffff, 0x0001ed09bead87c0}), Equals, true)
	c.Assert(fullHouse.IsIdentical(decimal.Decimal{0xffffffffffffffff, 0x3040ffffffffffff}), Equals, true)
}

func (s *S) TestDecimalFromStringExponentNormalization(c *C) {
	trailingZeros := decimal.Parse("1000000000000000000000000000000000000000");
	oneNormalize := decimal.Parse("10000000000000000000000000000000000");
	noNormalize := decimal.Parse("1000000000000000000000000000000000");
	aDisaster := decimal.Parse("10000000000000000000000000000000000000000000000000000000000000000000");

	zero := decimal.Parse("1E-6177");

	c.Assert(trailingZeros.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x304c314dc6448d93}), Equals, true)
	c.Assert(oneNormalize.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3042314dc6448d93}), Equals, true)
	c.Assert(noNormalize.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3040314dc6448d93}), Equals, true)
	c.Assert(aDisaster.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3084314dc6448d93}), Equals, true)
	c.Assert(zero.IsIdentical(decimal.Decimal{0x0000000000000000, 0x0000000000000000}), Equals, true)
}

func (s *S) TestDecimalFromStringZeros(c *C) {
	zero := decimal.Parse("0");
	exponentZero := decimal.Parse("0e-611");
	largeExponent := decimal.Parse("0e+6000");
	negativeZero := decimal.Parse("-0e-1");

	c.Assert(zero.IsIdentical(decimal.Decimal{0x0000000000000000, 0x3040000000000000}), Equals, true)
	c.Assert(exponentZero.IsIdentical(decimal.Decimal{0x0000000000000000, 0x2b7a000000000000}), Equals, true)
	c.Assert(largeExponent.IsIdentical(decimal.Decimal{0x0000000000000000, 0x5f20000000000000}), Equals, true)
	c.Assert(negativeZero.IsIdentical(decimal.Decimal{0x0000000000000000, 0xb03e000000000000}), Equals, true)
}

func (s *S) TestDecimalFromStringRound(c *C) {
	truncate := decimal.Parse("10E-6177");
	up := decimal.Parse("15E-6177");
	checkTieUp := decimal.Parse("251E-6178");
	checkTieTrunc := decimal.Parse("250E-6178");

	extraDigitUp := decimal.Parse("10000000000000000000000000000000006");
	extraDigitDown := decimal.Parse("10000000000000000000000000000000003");
	extraDigitTie := decimal.Parse("10000000000000000000000000000000005");
	extraDigitTieBreak := decimal.Parse("100000000000000000000000000000000051");

	tooBig := decimal.Parse("10000000000000000000000000000000006E6111");

	largestBinary := decimal.Parse("12980742146337069071326240823050239");

	roundPropagate := decimal.Parse("99999999999999999999999999999999999");
	roundPropagateLarge := decimal.Parse("9999999999999999999999999999999999999999999999999999999999999999999");
	notInf := decimal.Parse("9999999999999999999999999999999999E6111");
	roundPropagateInf := decimal.Parse("99999999999999999999999999999999999E6144");

	c.Assert(truncate.IsIdentical(decimal.Decimal{0x0000000000000001, 0x0000000000000000}), Equals, true)
	c.Assert(up.IsIdentical(decimal.Decimal{0x0000000000000002, 0x0000000000000000}), Equals, true)
	c.Assert(checkTieUp.IsIdentical(decimal.Decimal{0x0000000000000003, 0x0000000000000000}), Equals, true)
	c.Assert(checkTieTrunc.IsIdentical(decimal.Decimal{0x0000000000000002, 0x0000000000000000}), Equals, true)

	c.Assert(extraDigitUp.IsIdentical(decimal.Decimal{0x38c15b0a00000001, 0x3042314dc6448d93}), Equals, true)
	c.Assert(extraDigitDown.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3042314dc6448d93}), Equals, true)
	c.Assert(extraDigitTie.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3042314dc6448d93}), Equals, true)
	c.Assert(extraDigitTieBreak.IsIdentical(decimal.Decimal{0x38c15b0a00000001, 0x3044314dc6448d93}), Equals, true)

	c.Assert(tooBig.IsIdentical(decimal.Decimal{0x0000000000000000, 0x7800000000000000}), Equals, true)
	c.Assert(largestBinary.IsIdentical(decimal.Decimal{0x0000000000000000, 0x3042400000000000}), Equals, true)
	c.Assert(roundPropagate.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3044314dc6448d93}), Equals, true)
	c.Assert(roundPropagateLarge.IsIdentical(decimal.Decimal{0x38c15b0a00000000, 0x3084314dc6448d93}), Equals, true)
	c.Assert(notInf.IsIdentical(decimal.Decimal{0x378d8e63ffffffff, 0x5fffed09bead87c0}), Equals, true)
	c.Assert(roundPropagateInf.IsIdentical(decimal.Decimal{0x0000000000000000, 0x7800000000000000}), Equals, true)
}
