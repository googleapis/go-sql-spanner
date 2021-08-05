// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"bytes"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"encoding/gob"
	"math/big"
	"testing"
	"time"
)

func TestUpdateChecksum(t *testing.T) {
	buffer1 := &bytes.Buffer{}
	enc1 := gob.NewEncoder(buffer1)
	buffer2 := &bytes.Buffer{}
	enc2 := gob.NewEncoder(buffer2)
	buffer3 := &bytes.Buffer{}
	enc3 := gob.NewEncoder(buffer3)

	row1, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp",
		},
		[]interface{}{
			true, int64(1), 3.14, numeric("6.626"), "test", []byte("testbytes"), civil.Date{Year: 2021, Month: 8, Day: 5}, time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1", "test2"}, [][]byte{[]byte("testbytes1"), []byte("testbytes1")},
			[]civil.Date{{Year: 2021, Month: 8, Day: 5}, {Year: 2021, Month: 8, Day: 6}},
			[]time.Time{time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC), time.Date(2021, 8, 6, 13, 19, 23, 123456789, time.UTC)},
		},
	)
	if err != nil {
		t.Fatalf("could not create row 1: %v", err)
	}
	// row2 is different from row1
	row2, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp",
		},
		[]interface{}{
			true, int64(2), 6.626, numeric("3.14"), "test2", []byte("testbytes2"), civil.Date{Year: 2020, Month: 8, Day: 5}, time.Date(2020, 8, 5, 13, 19, 23, 123456789, time.UTC),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1_", "test2_"}, [][]byte{[]byte("testbytes1_"), []byte("testbytes1_")},
			[]civil.Date{{Year: 2020, Month: 8, Day: 5}, {Year: 2020, Month: 8, Day: 6}},
			[]time.Time{time.Date(2020, 8, 5, 13, 19, 23, 123456789, time.UTC), time.Date(2020, 8, 6, 13, 19, 23, 123456789, time.UTC)},
		},
	)
	if err != nil {
		t.Fatalf("could not create row 2: %v", err)
	}
	// row3 is equal to row1.
	row3, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp",
		},
		[]interface{}{
			true, int64(1), 3.14, numeric("6.626"), "test", []byte("testbytes"), civil.Date{Year: 2021, Month: 8, Day: 5}, time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1", "test2"}, [][]byte{[]byte("testbytes1"), []byte("testbytes1")},
			[]civil.Date{{Year: 2021, Month: 8, Day: 5}, {Year: 2021, Month: 8, Day: 6}},
			[]time.Time{time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC), time.Date(2021, 8, 6, 13, 19, 23, 123456789, time.UTC)},
		},
	)
	if err != nil {
		t.Fatalf("could not create row 3: %v", err)
	}
	var initial [32]byte
	checksum1, err := updateChecksum(enc1, buffer1, initial, row1)
	if err != nil {
		t.Fatalf("could not calculate checksum 1: %v", err)
	}
	checksum2, err := updateChecksum(enc2, buffer2, initial, row2)
	if err != nil {
		t.Fatalf("could not calculate checksum 2: %v", err)
	}
	checksum3, err := updateChecksum(enc3, buffer3, initial, row3)
	if err != nil {
		t.Fatalf("could not calculate checksum 3: %v", err)
	}
	// row1 and row2 are different, so the checksums should be different.
	if checksum1 == checksum2 {
		t.Fatalf("checksum1 should not be equal to checksum2")
	}
	// row1 and row3 are equal, and should return the same checksum.
	if checksum1 != checksum3 {
		t.Fatalf("checksum1 should be equal to checksum3")
	}

	// Updating checksums 1 and 3 with the data from row 2 should also produce
	// the same checksum.
	checksum1_2, err := updateChecksum(enc1, buffer1, checksum1, row2)
	if err != nil {
		t.Fatalf("could not calculate checksum 1_2: %v", err)
	}
	checksum3_2, err := updateChecksum(enc3, buffer3, checksum3, row2)
	if err != nil {
		t.Fatalf("could not calculate checksum 1_2: %v", err)
	}
	if checksum1_2 != checksum3_2 {
		t.Fatalf("checksum1_2 should be equal to checksum3_2")
	}

	// The combination of row 3 and 2 will produce a different checksum than the
	// combination 2 and 3, because they are in a different order.
	checksum2_3, err := updateChecksum(enc2, buffer2, checksum2, row3)
	if err != nil {
		t.Fatalf("could not calculate checksum 2_3: %v", err)
	}
	if checksum2_3 == checksum3_2 {
		t.Fatalf("checksum2_3 should not be equal to checksum3_2")
	}
}

func TestUpdateChecksumForNullValues(t *testing.T) {
	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)

	row, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp",
		},
		[]interface{}{
			spanner.NullBool{}, spanner.NullInt64{}, spanner.NullFloat64{}, spanner.NullNumeric{}, spanner.NullString{},
			[]byte(nil), spanner.NullDate{}, spanner.NullTime{},
			// Note: The following arrays all contain one NULL value.
			[]spanner.NullBool{{}}, []spanner.NullInt64{{}}, []spanner.NullFloat64{{}}, []spanner.NullNumeric{{}},
			[]spanner.NullString{{}}, [][]byte{[]byte(nil)}, []spanner.NullDate{{}}, []spanner.NullTime{{}},
		},
	)
	if err != nil {
		t.Fatalf("could not create row: %v", err)
	}
	var initial [32]byte
	// Create the initial checksum.
	checksum, err := updateChecksum(enc, buffer, initial, row)
	if err != nil {
		t.Fatalf("could not calculate checksum 1: %v", err)
	}
	// The calculated checksum should not be equal to the initial value, even though it only
	// contains null values.
	if checksum == initial {
		t.Fatalf("checksum value should not be equal to the initial value")
	}
	// Calculating the same checksum again should yield the same result.
	buffer2 := &bytes.Buffer{}
	enc2 := gob.NewEncoder(buffer2)
	checksum2, err := updateChecksum(enc2, buffer2, initial, row)
	if checksum != checksum2 {
		t.Fatalf("recalculated checksum does not match the initial calculation")
	}
}
