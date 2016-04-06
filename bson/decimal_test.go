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

package bson_test

import (
	"encoding/hex"
	"encoding/json"

	"gopkg.in/mgo.v2-unstable/bson"

	. "gopkg.in/check.v1"
	"regexp"
	"strings"
)

// --------------------------------------------------------------------------
// Decimal tests

type decimalTests struct {
	Valid []struct {
		Description string `json:"description"`
		Subject     string `json:"subject"`
		String      string `json:"string"`
		ExtJSON     string `json:"extjson"`
	} `json:"valid"`

	ParseErrors []struct{
		Description string `json:"description"`
		Subject     string `json:"subject"`
	} `json:"parseErrors"`
}

func extJSONRepr(s string) string {
	var value struct {
		D struct {
			Repr string `json:"$numberDecimal"`
		} `json:"d"`
	}
	err := json.Unmarshal([]byte(s), &value)
	if err != nil {
		panic(err)
	}
	return value.D.Repr
}

func (s *S) TestDecimalTests(c *C) {
	var tests decimalTests
	err := json.Unmarshal([]byte(decimalTestsJSON), &tests)
	c.Assert(err, IsNil)

	for _, test := range tests.Valid {
		c.Logf("Running decimal128 test: %s (string %q)", test.Description, test.String)
		subject, err := hex.DecodeString(test.Subject)
		var value struct{ D interface{} }

		// Unmarshal value from BSON data.
		err = bson.Unmarshal(subject, &value)
		c.Assert(err, IsNil)
		d, isDecimal := value.D.(bson.Decimal128)
		c.Assert(isDecimal, Equals, true)

		// Generate canonical representation.
		c.Assert(d.String(), Equals, test.String)

		// Parse Go variant representation (Inf vs. Infinity).
		parsed, err := bson.ParseDecimal128(test.String)
		c.Assert(err, IsNil)
		c.Assert(parsed.String(), Equals, test.String)

		// Parse original output representation.
		parsed, err = bson.ParseDecimal128(test.String)
		c.Assert(err, IsNil)
		c.Assert(parsed.String(), Equals, test.String)

		// Parse non-canonical input representation.
		parsed, err = bson.ParseDecimal128(extJSONRepr(test.ExtJSON))
		c.Assert(err, IsNil)
		c.Assert(parsed.String(), Equals, test.String)

		// Marshal back into BSON data.
		data, err := bson.Marshal(value)
		c.Assert(err, IsNil)
		c.Assert(strings.ToUpper(hex.EncodeToString(data)), Equals, test.Subject)
	}

	for _, test := range tests.ParseErrors {
		c.Logf("Running decimal128 parse error test: %s (string %q)", test.Description, test.Subject)

		_, err := bson.ParseDecimal128(test.Subject)
		c.Assert(err, ErrorMatches, `cannot parse "`+regexp.QuoteMeta(test.Subject)+`" as a Decimal128`)
	}
}

const decBenchNum = "9.999999999999999999999999999999999E+6144"

func (s *S) BenchmarkDecimal128String(c *C) {
	d, err := bson.ParseDecimal128(decBenchNum)
	c.Assert(err, IsNil)
	c.Assert(d.String(), Equals, decBenchNum)

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		d.String()
	}
}

func (s *S) BenchmarkDecimal128Parse(c *C) {
	var err error
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		_, err = bson.ParseDecimal128(decBenchNum)
	}
	if err != nil {
		panic(err)
	}
}

// Originally from https://goo.gl/HWFTAw

var decimalTestsJSON = `
{
    "description": "Decimal128",
    "bson_type": "0x13",
    "test_key": "d",
    "valid": [
        {
            "description": "Special - Canonical NaN",
            "subject": "180000001364000000000000000000000000000000007C00",
            "string": "NaN",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"NaN\"}}"
        },
        {
            "description": "Special - Negative NaN",
            "subject": "18000000136400000000000000000000000000000000FC00",
            "string": "NaN",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"NaN\"}}"
        },
        {
            "description": "Special - Canonical SNaN",
            "subject": "180000001364000000000000000000000000000000007E00",
            "string": "NaN",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"NaN\"}}"
        },
        {
            "description": "Special - Negative SNaN",
            "subject": "18000000136400000000000000000000000000000000FE00",
            "string": "NaN",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"NaN\"}}"
        },
        {
            "description": "Special - NaN with a payload",
            "subject": "180000001364001200000000000000000000000000007E00",
            "string": "NaN",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"NaN\"}}"
        },
        {
            "description": "Special - Canonical Positive Infinity",
            "subject": "180000001364000000000000000000000000000000007800",
            "string": "Inf",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"Inf\"}}"
        },
        {
            "description": "Special - Canonical Negative Infinity",
            "subject": "18000000136400000000000000000000000000000000F800",
            "string": "-Inf",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-Inf\"}}"
        },
        {
            "description": "Special - Invalid representation treated as 0",
            "subject": "180000001364000000000000000000000000000000106C00",
            "string": "0",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0\"}}"
        },
        {
            "description": "Special - Invalid representation treated as -0",
            "subject": "18000000136400DCBA9876543210DEADBEEF00000010EC00",
            "string": "-0",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-0\"}}"
        },
        {
            "description": "Special - Invalid representation treated as 0E3",
            "subject": "18000000136400FFFFFFFFFFFFFFFFFFFFFFFFFFFF116C00",
            "string": "0E+3",
            "from_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0E+3\"}}"
        },
        {
            "description": "Regular - Adjusted Exponent Limit",
            "subject": "18000000136400F2AF967ED05C82DE3297FF6FDE3CF22F00",
            "string": "0.000001234567890123456789012345678901234",
            "extjson": "{\"d\": { \"$numberDecimal\": \"0.000001234567890123456789012345678901234\" }}"
        },
        {
            "description": "Regular - Smallest",
            "subject": "18000000136400D204000000000000000000000000343000",
            "string": "0.001234",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0.001234\"}}"
        },
        {
            "description": "Regular - Smallest with Trailing Zeros",
            "subject": "1800000013640040EF5A07000000000000000000002A3000",
            "string": "0.00123400000",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0.00123400000\"}}"
        },
        {
            "description": "Regular - 0.1",
            "subject": "1800000013640001000000000000000000000000003E3000",
            "string": "0.1",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0.1\"}}"
        },
        {
            "description": "Regular - 0.1234567890123456789012345678901234",
            "subject": "18000000136400F2AF967ED05C82DE3297FF6FDE3CFC2F00",
            "string": "0.1234567890123456789012345678901234",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0.1234567890123456789012345678901234\"}}"
        },
        {
            "description": "Regular - 0",
            "subject": "180000001364000000000000000000000000000000403000",
            "string": "0",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0\"}}"
        },
        {
            "description": "Regular - -0",
            "subject": "18000000136400000000000000000000000000000040B000",
            "string": "-0",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-0\"}}"
        },
        {
            "description": "Regular - -0.0",
            "subject": "1800000013640000000000000000000000000000003EB000",
            "string": "-0.0",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-0.0\"}}"
        },
        {
            "description": "Regular - 2",
            "subject": "180000001364000200000000000000000000000000403000",
            "string": "2",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"2\"}}"
        },
        {
            "description": "Regular - 2.000",
            "subject": "18000000136400D0070000000000000000000000003A3000",
            "string": "2.000",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"2.000\"}}"
        },
        {
            "description": "Regular - Largest",
            "subject": "18000000136400F2AF967ED05C82DE3297FF6FDE3C403000",
            "string": "1234567890123456789012345678901234",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1234567890123456789012345678901234\"}}"
        },
        {
            "description": "Scientific - Tiniest",
            "subject": "18000000136400FFFFFFFF638E8D37C087ADBE09ED010000",
            "string": "9.999999999999999999999999999999999E-6143",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"9.999999999999999999999999999999999E-6143\"}}"
        },
        {
            "description": "Scientific - Tiny",
            "subject": "180000001364000100000000000000000000000000000000",
            "string": "1E-6176",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1E-6176\"}}"
        },
        {
            "description": "Scientific - Negative Tiny",
            "subject": "180000001364000100000000000000000000000000008000",
            "string": "-1E-6176",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-1E-6176\"}}"
        },
        {
            "description": "Scientific - Adjusted Exponent Limit",
            "subject": "18000000136400F2AF967ED05C82DE3297FF6FDE3CF02F00",
            "string": "1.234567890123456789012345678901234E-7",
            "extjson": "{\"d\": { \"$numberDecimal\": \"1.234567890123456789012345678901234E-7\" }}"
        },
        {
            "description": "Scientific - Fractional",
            "subject": "1800000013640064000000000000000000000000002CB000",
            "string": "-1.00E-8",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-1.00E-8\"}}"
        },
        {
            "description": "Scientific - 0 with Exponent",
            "subject": "180000001364000000000000000000000000000000205F00",
            "string": "0E+6000",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0E+6000\"}}"
        },
        {
            "description": "Scientific - 0 with Negative Exponent",
            "subject": "1800000013640000000000000000000000000000007A2B00",
            "string": "0E-611",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"0E-611\"}}"
        },
        {
            "description": "Scientific - No Decimal with Signed Exponent",
            "subject": "180000001364000100000000000000000000000000463000",
            "string": "1E+3",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1E+3\"}}"
        },
        {
            "description": "Scientific - Trailing Zero",
            "subject": "180000001364001A04000000000000000000000000423000",
            "string": "1.050E+4",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1.050E+4\"}}"
        },
        {
            "description": "Scientific - With Decimal",
            "subject": "180000001364006900000000000000000000000000423000",
            "string": "1.05E+3",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1.05E+3\"}}"
        },
        {
            "description": "Scientific - Full",
            "subject": "18000000136400FFFFFFFFFFFFFFFFFFFFFFFFFFFF403000",
            "string": "5192296858534827628530496329220095",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"5192296858534827628530496329220095\"}}"
        },
        {
            "description": "Scientific - Large",
            "subject": "18000000136400000000000A5BC138938D44C64D31FE5F00",
            "string": "1.000000000000000000000000000000000E+6144",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1.000000000000000000000000000000000E+6144\"}}"
        },
        {
            "description": "Scientific - Largest",
            "subject": "18000000136400FFFFFFFF638E8D37C087ADBE09EDFF5F00",
            "string": "9.999999999999999999999999999999999E+6144",
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"9.999999999999999999999999999999999E+6144\"}}"
        },
        {
            "description": "Non-Canonical Parsing - Exponent Normalization",
            "subject": "1800000013640064000000000000000000000000002CB000",
            "string": "-1.00E-8",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-100E-10\"}}"
        },
        {
            "description": "Non-Canonical Parsing - Unsigned Positive Exponent",
            "subject": "180000001364000100000000000000000000000000463000",
            "string": "1E+3",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1E3\"}}"
        },
        {
            "description": "Non-Canonical Parsing - Lowercase Exponent Identifier",
            "subject": "180000001364000100000000000000000000000000463000",
            "string": "1E+3",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"1e+3\"}}"
        },
        {
            "description": "Non-Canonical Parsing - Long Significand with Exponent",
            "subject": "1800000013640079D9E0F9763ADA429D0200000000583000",
            "string": "1.2345689012345789012345E+34",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"12345689012345789012345E+12\"}}"
        },
        {
            "description": "Non-Canonical Parsing - Positive Sign",
            "subject": "18000000136400F2AF967ED05C82DE3297FF6FDE3C403000",
            "string": "1234567890123456789012345678901234",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"+1234567890123456789012345678901234\"}}"
        },
        {
            "description": "Non-Canonical Parsing - Long Decimal String",
            "subject": "180000001364000100000000000000000000000000722800",
            "string": "1E-999",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \".000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001\"}}"
        },
        {
            "description": "Non-Canonical Parsing - nan",
            "subject": "180000001364000000000000000000000000000000007C00",
            "string": "NaN",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"nan\"}}"
        },
        {
            "description": "Non-Canonical Parsing - nAn",
            "subject": "180000001364000000000000000000000000000000007C00",
            "string": "NaN",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"nAn\"}}"
        },
        {
            "description": "Non-Canonical Parsing - +infinity",
            "subject": "180000001364000000000000000000000000000000007800",
            "string": "Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"+infinity\"}}"
        },
        {
            "description": "Non-Canonical Parsing - infinity",
            "subject": "180000001364000000000000000000000000000000007800",
            "string": "Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"infinity\"}}"
        },
        {
            "description": "Non-Canonical Parsing - infiniTY",
            "subject": "180000001364000000000000000000000000000000007800",
            "string": "Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"infiniTY\"}}"
        },
        {
            "description": "Non-Canonical Parsing - inf",
            "subject": "180000001364000000000000000000000000000000007800",
            "string": "Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"inf\"}}"
        },
        {
            "description": "Non-Canonical Parsing - inF",
            "subject": "180000001364000000000000000000000000000000007800",
            "string": "Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"inF\"}}"
        },
        {
            "description": "Non-Canonical Parsing - -infinity",
            "subject": "18000000136400000000000000000000000000000000F800",
            "string": "-Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-infinity\"}}"
        },
        {
            "description": "Non-Canonical Parsing - -infiniTy",
            "subject": "18000000136400000000000000000000000000000000F800",
            "string": "-Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-infiniTy\"}}"
        },
        {
            "description": "Non-Canonical Parsing - -Inf",
            "subject": "18000000136400000000000000000000000000000000F800",
            "string": "-Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-Infinity\"}}"
        },
        {
            "description": "Non-Canonical Parsing - -inf",
            "subject": "18000000136400000000000000000000000000000000F800",
            "string": "-Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-inf\"}}"
        },
        {
            "description": "Non-Canonical Parsing - -inF",
            "subject": "18000000136400000000000000000000000000000000F800",
            "string": "-Inf",
            "to_extjson": false,
            "extjson": "{\"d\" : {\"$numberDecimal\" : \"-inF\"}}"
        }
    ],
    "parseErrors": [
        {
            "description": "Too many significand digits",
            "subject": "100000000000000000000000000000000000000000000000000000000001"
        },
        {
            "description": "Too many significand digits",
            "subject": "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        },
        {
            "description": "Too many significand digits",
            "subject": ".100000000000000000000000000000000000000000000000000000000000"
        },
        {
            "description": "Incomplete Exponent",
            "subject": "1e"
        },
        {
            "description": "Exponent at the beginning",
            "subject": "E01"
        },
        {
            "description": "Exponent too large",
            "subject": "1E6112"
        },
        {
            "description": "Exponent too small",
            "subject": "1E-6177"
        },
        {
            "description": "Just a decimal place",
            "subject": "."
        },
        {
            "description": "2 decimal places",
            "subject": "..3"
        },
        {
            "description": "2 decimal places",
            "subject": ".13.3"
        },
        {
            "description": "2 decimal places",
            "subject": "1..3"
        },
        {
            "description": "2 decimal places",
            "subject": "1.3.4"
        },
        {
            "description": "2 decimal places",
            "subject": "1.34."
        },
        {
            "description": "Decimal with no digits",
            "subject": ".e"
        },
        {
            "description": "2 signs",
            "subject": "+-32.4"
        },
        {
            "description": "2 signs",
            "subject": "-+32.4"
        },
        {
            "description": "2 negative signs",
            "subject": "--32.4"
        },
        {
            "description": "2 negative signs",
            "subject": "-32.-4"
        },
        {
            "description": "End in negative sign",
            "subject": "32.0-"
        },
        {
            "description": "2 negative signs",
            "subject": "32.4E--21"
        },
        {
            "description": "2 negative signs",
            "subject": "32.4E-2-1"
        },
        {
            "description": "2 signs",
            "subject": "32.4E+-21"
        },
        {
            "description": "Empty string",
            "subject": ""
        },
        {
            "description": "leading white space positive number",
            "subject": " 1"
        },
        {
            "description": "leading white space negative number",
            "subject": " -1"
        },
        {
            "description": "trailing white space",
            "subject": "1 "
        },
        {
            "description": "Invalid",
            "subject": "E"
        },
        {
            "description": "Invalid",
            "subject": "invalid"
        },
        {
            "description": "Invalid",
            "subject": "i"
        },
        {
            "description": "Invalid",
            "subject": "in"
        },
        {
            "description": "Invalid",
            "subject": "-in"
        },
        {
            "description": "Invalid",
            "subject": "Na"
        },
        {
            "description": "Invalid",
            "subject": "-Na"
        },
        {
            "description": "Invalid",
            "subject": "1.23abc"
        },
        {
            "description": "Invalid",
            "subject": "1.23abcE+02"
        },
        {
            "description": "Invalid",
            "subject": "1.23E+0aabs2"
        }
    ]
}
`
