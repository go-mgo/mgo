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
	c.Assert(ten.String(2), Equals, "10.00")

	tenth, err := decimal.Parse("0.1")
	c.Assert(err, IsNil)
	c.Assert(tenth.String(2), Equals, "0.10")

	exponent, err := decimal.Parse("1e-5")
	c.Assert(err, IsNil)
	c.Assert(exponent.String(5), Equals, "0.00001")

	infinity, err := decimal.Parse("Inf")
	c.Assert(err, IsNil)
	c.Assert(infinity.String(1), Equals, "Infinity")

	ninfinity, err := decimal.Parse("-Inf")
	c.Assert(err, IsNil)
	c.Assert(ninfinity.String(1), Equals, "-Infinity")

	failure, err := decimal.Parse("I am not a number!")
	c.Assert(err, ErrorMatches, "Cannot create Decimal 'I am not a number!'")
	c.Assert(failure.String(1), Equals, "NaN")
}
