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

func (s *S) TestDecimalStringRoundTrip(c *C) {
	ten, err := decimal.Parse("10")
	c.Assert(err, IsNil)
	c.Assert(ten.String(), Equals, "10")

	tenth, err := decimal.Parse("0.1")
	c.Assert(err, IsNil)
	c.Assert(tenth.String(), Equals, "0.1")

	twoTenths, err := decimal.Parse("0.2")
	c.Assert(err, IsNil)
	c.Assert(twoTenths.String(), Equals, "0.2")

	nineTenths, err := decimal.Parse("0.9")
	c.Assert(err, IsNil)
	c.Assert(nineTenths.String(), Equals, "0.9")

	thousandth, err := decimal.Parse("0.001")
	c.Assert(err, IsNil)
	c.Assert(thousandth.String(), Equals, "0.001")

	exponent, err := decimal.Parse("1e-5")
	c.Assert(err, IsNil)
	c.Assert(exponent.String(), Equals, "0.00001")

	exponent2, err := decimal.Parse("2e-50")
	c.Assert(err, IsNil)
	c.Assert(exponent2.String(), Equals, "0.00000000000000000000000000000000000000000000000002")

	float2, err := decimal.Parse("0.00000000000000000000000000000000000000000000000002")
	c.Assert(err, IsNil)
	c.Assert(float2.String(), Equals, "0.00000000000000000000000000000000000000000000000002")

	infinity, err := decimal.Parse("Inf")
	c.Assert(err, IsNil)
	c.Assert(infinity.String(), Equals, "Infinity")

	ninfinity, err := decimal.Parse("-Inf")
	c.Assert(err, IsNil)
	c.Assert(ninfinity.String(), Equals, "-Infinity")

	failure, err := decimal.Parse("I am not a number!")
	c.Assert(err, ErrorMatches, "Cannot create Decimal 'I am not a number!'")
	c.Assert(failure.String(), Equals, "NaN")
}
