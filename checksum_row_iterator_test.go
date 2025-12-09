// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestUpdateChecksum(t *testing.T) {
	row1, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			true, int64(1), 3.14, numeric("6.626"), "test", []byte("testbytes"), civil.Date{Year: 2021, Month: 8, Day: 5},
			time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
			nullJson(true, `"key": "value", "other-key": ["value1", "value2"]}`),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1", "test2"}, [][]byte{[]byte("testbytes1"), []byte("testbytes1")},
			[]civil.Date{{Year: 2021, Month: 8, Day: 5}, {Year: 2021, Month: 8, Day: 6}},
			[]time.Time{
				time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
				time.Date(2021, 8, 6, 13, 19, 23, 123456789, time.UTC),
			},
			[]spanner.NullJSON{
				nullJson(true, `"key1": "value1", "other-key1": ["value1", "value2"]}`),
				nullJson(true, `"key2": "value2", "other-key2": ["value1", "value2"]}`),
			},
		},
	)
	if err != nil {
		t.Fatalf("could not create row 1: %v", err)
	}
	// row2 is different from row1
	row2, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			true, int64(2), 6.626, numeric("3.14"), "test2", []byte("testbytes2"), civil.Date{Year: 2020, Month: 8, Day: 5},
			time.Date(2020, 8, 5, 13, 19, 23, 123456789, time.UTC),
			nullJson(true, `"key": "other-value", "other-key": ["other-value1", "other-value2"]}`),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1_", "test2_"}, [][]byte{[]byte("testbytes1_"), []byte("testbytes1_")},
			[]civil.Date{{Year: 2020, Month: 8, Day: 5}, {Year: 2020, Month: 8, Day: 6}},
			[]time.Time{
				time.Date(2020, 8, 5, 13, 19, 23, 123456789, time.UTC),
				time.Date(2020, 8, 6, 13, 19, 23, 123456789, time.UTC),
			},
			[]spanner.NullJSON{
				nullJson(true, `"key1": "other-value1", "other-key1": ["other-value1", "other-value2"]}`),
				nullJson(true, `"key2": "other-value2", "other-key2": ["other-value1", "other-value2"]}`),
			},
		},
	)
	if err != nil {
		t.Fatalf("could not create row 2: %v", err)
	}
	// row3 is equal to row1.
	row3, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			true, int64(1), 3.14, numeric("6.626"), "test", []byte("testbytes"), civil.Date{Year: 2021, Month: 8, Day: 5},
			time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
			nullJson(true, `"key": "value", "other-key": ["value1", "value2"]}`),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1", "test2"}, [][]byte{[]byte("testbytes1"), []byte("testbytes1")},
			[]civil.Date{{Year: 2021, Month: 8, Day: 5}, {Year: 2021, Month: 8, Day: 6}},
			[]time.Time{
				time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
				time.Date(2021, 8, 6, 13, 19, 23, 123456789, time.UTC),
			},
			[]spanner.NullJSON{
				nullJson(true, `"key1": "value1", "other-key1": ["value1", "value2"]}`),
				nullJson(true, `"key2": "value2", "other-key2": ["value1", "value2"]}`),
			},
		},
	)
	if err != nil {
		t.Fatalf("could not create row 3: %v", err)
	}

	hash1 := sha256.New()
	err = updateChecksum(hash1, row1)
	if err != nil {
		t.Fatalf("could not calculate checksum 1: %v", err)
	}
	checksum1 := hash1.Sum(nil)

	hash2 := sha256.New()
	err = updateChecksum(hash2, row2)
	if err != nil {
		t.Fatalf("could not calculate checksum 2: %v", err)
	}
	checksum2 := hash2.Sum(nil)

	hash3 := sha256.New()
	err = updateChecksum(hash3, row3)
	if err != nil {
		t.Fatalf("could not calculate checksum 3: %v", err)
	}
	// row1 and row2 are different, so the checksums should be different.
	checksum3 := hash3.Sum(nil)

	if bytes.Equal(checksum1, checksum2) {
		t.Fatalf("checksum1 should not be equal to checksum2")
	}
	// row1 and row3 are equal, and should return the same checksum.
	if !bytes.Equal(checksum1, checksum3) {
		t.Fatalf("checksum1 should be equal to checksum3")
	}

	// Updating checksums 1 and 3 with the data from row 2 should also produce
	// the same checksum.
	err = updateChecksum(hash1, row2)
	if err != nil {
		t.Fatalf("could not calculate checksum 1_2: %v", err)
	}
	checksum1_2 := hash1.Sum(nil)

	err = updateChecksum(hash3, row2)
	if err != nil {
		t.Fatalf("could not calculate checksum 3_2: %v", err)
	}
	checksum3_2 := hash3.Sum(nil)

	if !bytes.Equal(checksum1_2, checksum3_2) {
		t.Fatalf("checksum1_2 should be equal to checksum3_2")
	}

	// The combination of row 3 and 2 will produce a different checksum than the
	// combination 2 and 3, because they are in a different order.
	err = updateChecksum(hash2, row3)
	if err != nil {
		t.Fatalf("could not calculate checksum 2_3: %v", err)
	}
	checksum2_3 := hash2.Sum(nil)

	if bytes.Equal(checksum2_3, checksum3_2) {
		t.Fatalf("checksum2_3 should not be equal to checksum3_2")
	}
}

func TestUpdateChecksumForNullValues(t *testing.T) {
	row, err := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			spanner.NullBool{}, spanner.NullInt64{}, spanner.NullFloat64{}, spanner.NullNumeric{}, spanner.NullString{},
			[]byte(nil), spanner.NullDate{}, spanner.NullTime{}, spanner.NullJSON{},
			// Note: The following arrays all contain one NULL value.
			[]spanner.NullBool{{}}, []spanner.NullInt64{{}}, []spanner.NullFloat64{{}}, []spanner.NullNumeric{{}},
			[]spanner.NullString{{}}, [][]byte{[]byte(nil)}, []spanner.NullDate{{}}, []spanner.NullTime{{}},
			[]spanner.NullJSON{{}},
		},
	)
	if err != nil {
		t.Fatalf("could not create row: %v", err)
	}
	hash1 := sha256.New()
	initial := hash1.Sum(nil)
	// Create the initial checksum.
	err = updateChecksum(hash1, row)
	if err != nil {
		t.Fatalf("could not calculate checksum 1: %v", err)
	}
	checksum1 := hash1.Sum(nil)
	// The calculated checksum should not be equal to the initial value, even though it only
	// contains null values.
	if bytes.Equal(initial, checksum1) {
		t.Fatalf("checksum value should not be equal to the initial value")
	}
	// Calculating the same checksum again should yield the same result.
	hash2 := sha256.New()
	err = updateChecksum(hash2, row)
	if err != nil {
		t.Fatalf("failed to update checksum: %v", err)
	}
	checksum2 := hash2.Sum(nil)
	if !bytes.Equal(checksum1, checksum2) {
		t.Fatalf("recalculated checksum does not match the initial calculation")
	}
}

func BenchmarkChecksumRowIterator(b *testing.B) {
	row1, _ := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			true, int64(1), 3.14, numeric("6.626"), "test", []byte("testbytes"), civil.Date{Year: 2021, Month: 8, Day: 5},
			time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
			nullJson(true, `"key": "value", "other-key": ["value1", "value2"]}`),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1", "test2"}, [][]byte{[]byte("testbytes1"), []byte("testbytes1")},
			[]civil.Date{{Year: 2021, Month: 8, Day: 5}, {Year: 2021, Month: 8, Day: 6}},
			[]time.Time{
				time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
				time.Date(2021, 8, 6, 13, 19, 23, 123456789, time.UTC),
			},
			[]spanner.NullJSON{
				nullJson(true, `"key1": "value1", "other-key1": ["value1", "value2"]}`),
				nullJson(true, `"key2": "value2", "other-key2": ["value1", "value2"]}`),
			},
		},
	)
	row2, _ := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			true, int64(2), 6.626, numeric("3.14"), "test2", []byte("testbytes2"), civil.Date{Year: 2020, Month: 8, Day: 5},
			time.Date(2020, 8, 5, 13, 19, 23, 123456789, time.UTC),
			nullJson(true, `"key": "other-value", "other-key": ["other-value1", "other-value2"]}`),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1_", "test2_"}, [][]byte{[]byte("testbytes1_"), []byte("testbytes1_")},
			[]civil.Date{{Year: 2020, Month: 8, Day: 5}, {Year: 2020, Month: 8, Day: 6}},
			[]time.Time{
				time.Date(2020, 8, 5, 13, 19, 23, 123456789, time.UTC),
				time.Date(2020, 8, 6, 13, 19, 23, 123456789, time.UTC),
			},
			[]spanner.NullJSON{
				nullJson(true, `"key1": "other-value1", "other-key1": ["other-value1", "other-value2"]}`),
				nullJson(true, `"key2": "other-value2", "other-key2": ["other-value1", "other-value2"]}`),
			},
		},
	)
	row3, _ := spanner.NewRow(
		[]string{
			"ColBool", "ColInt64", "ColFloat64", "ColNumeric", "ColString", "ColBytes", "ColDate", "ColTimestamp", "ColJson",
			"ArrBool", "ArrInt64", "ArrFloat64", "ArrNumeric", "ArrString", "ArrBytes", "ArrDate", "ArrTimestamp", "ArrJson",
		},
		[]interface{}{
			true, int64(1), 3.14, numeric("6.626"), "test", []byte("testbytes"), civil.Date{Year: 2021, Month: 8, Day: 5},
			time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
			nullJson(true, `"key": "value", "other-key": ["value1", "value2"]}`),
			[]bool{true, false}, []int64{1, 2}, []float64{3.14, 6.626}, []big.Rat{numeric("3.14"), numeric("6.626")},
			[]string{"test1", "test2"}, [][]byte{[]byte("testbytes1"), []byte("testbytes1")},
			[]civil.Date{{Year: 2021, Month: 8, Day: 5}, {Year: 2021, Month: 8, Day: 6}},
			[]time.Time{
				time.Date(2021, 8, 5, 13, 19, 23, 123456789, time.UTC),
				time.Date(2021, 8, 6, 13, 19, 23, 123456789, time.UTC),
			},
			[]spanner.NullJSON{
				nullJson(true, `"key1": "value1", "other-key1": ["value1", "value2"]}`),
				nullJson(true, `"key2": "value2", "other-key2": ["value1", "value2"]}`),
			},
		},
	)

	for b.Loop() {
		hash := sha256.New()
		if err := updateChecksum(hash, row1); err != nil {
			b.Fatal(err)
		}
		if err := updateChecksum(hash, row2); err != nil {
			b.Fatal(err)
		}
		if err := updateChecksum(hash, row3); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChecksumRowIteratorRandom(b *testing.B) {
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		resultSet := testutil.CreateRandomResultSet(numRows)
		columnNames := make([]string, len(resultSet.Metadata.RowType.Fields))
		for i := range columnNames {
			columnNames[i] = resultSet.Metadata.RowType.Fields[i].Name
		}
		var err error
		rows := make([]*spanner.Row, numRows)
		for row, values := range resultSet.Rows {
			columnValues := convertListValuesToColumnValues(resultSet.Metadata, values)
			c := make([]interface{}, len(columnValues))
			for i := range columnValues {
				c[i] = columnValues[i]
			}
			rows[row], err = spanner.NewRow(columnNames, c)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprintf("num-rows-%d", numRows), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				hash, err := createMetadataChecksum(resultSet.Metadata)
				if err != nil {
					b.Fatal(err)
				}
				for _, row := range rows {
					if err := updateChecksum(hash, row); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func convertListValuesToColumnValues(metadata *spannerpb.ResultSetMetadata, values *structpb.ListValue) []spanner.GenericColumnValue {
	res := make([]spanner.GenericColumnValue, len(values.Values))
	for i := range values.Values {
		res[i] = spanner.GenericColumnValue{Value: values.Values[i], Type: metadata.RowType.Fields[i].Type}
	}
	return res
}
