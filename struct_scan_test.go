package dalgo2sql

import (
	"database/sql"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestColumnKey(t *testing.T) {
	for _, name := range []string{"AreaSqKm", "areasqkm", "area_sq_km", "AREA_SQ_KM"} {
		if got := columnKey(name); got != "areasqkm" {
			t.Errorf("columnKey(%q) = %q", name, got)
		}
	}
}

type scanEmbedded struct {
	Promoted string
}

type scanPointerEmbedded struct {
	PointerPromoted string
}

type scanTarget struct {
	scanEmbedded
	*scanPointerEmbedded
	Name       string
	Tagged     string `db:"label,omitempty"`
	Hidden     string `db:"-"`
	hidden     string
	Duplicate  string
	Dup_licate string
}

func TestStructColumnsOf(t *testing.T) {
	for range 2 { // the second call is served from the cache
		columns := structColumnsOf(reflect.TypeOf(scanTarget{}))
		for _, want := range []string{"name", "label", "promoted", "pointerpromoted", "duplicate"} {
			if _, ok := columns[want]; !ok {
				t.Errorf("missing %q in %v", want, columns)
			}
		}
		for _, absent := range []string{"tagged", "hidden", "scanembedded", "scanpointerembedded"} {
			if _, ok := columns[absent]; ok {
				t.Errorf("unexpected %q in %v", absent, columns)
			}
		}
	}
	columns := structColumnsOf(reflect.TypeOf(scanTarget{}))
	if first := columns["duplicate"]; first[0] != reflect.TypeOf(scanTarget{}).NumField()-2 {
		t.Errorf("first field with a shared key must win, got index %v", first)
	}
}

func TestStructSetter_RejectsNonStructPointers(t *testing.T) {
	var nilTarget *scanTarget
	for name, target := range map[string]any{
		"nil":            nil,
		"struct value":   scanTarget{},
		"nil pointer":    nilTarget,
		"pointer to int": new(int),
		"map":            map[string]any{},
	} {
		if _, ok := structSetter(target, true); ok {
			t.Errorf("%s: want ok=false", name)
		}
	}
}

func TestStructSetter_Set(t *testing.T) {
	target := &scanTarget{}
	set, ok := structSetter(target, true)
	if !ok {
		t.Fatal("want ok")
	}
	if err := set("NAME", "x"); err != nil || target.Name != "x" {
		t.Errorf("case-insensitive name: %v %+v", err, target)
	}
	if err := set("label", "l"); err != nil || target.Tagged != "l" {
		t.Errorf("db tag: %v %+v", err, target)
	}
	if err := set("promoted", "p"); err != nil || target.Promoted != "p" {
		t.Errorf("promoted: %v %+v", err, target)
	}
	if err := set("nothing_like_it", "x"); err != nil {
		t.Errorf("ignored column: %v", err)
	}
	err := set("PointerPromoted", "x")
	if err == nil || !strings.Contains(err.Error(), `column "PointerPromoted"`) {
		t.Errorf("nil embedded pointer: %v", err)
	}
	target.scanPointerEmbedded = &scanPointerEmbedded{}
	if err = set("PointerPromoted", "x"); err != nil || target.PointerPromoted != "x" {
		t.Errorf("allocated embedded pointer: %v %+v", err, target)
	}
	err = set("Name", struct{}{})
	if err == nil || !strings.Contains(err.Error(), `column "Name"`) {
		t.Errorf("unassignable value: %v", err)
	}

	strict, _ := structSetter(target, false)
	err = strict("nothing_like_it", "x")
	if err == nil || !strings.Contains(err.Error(), "no corresponding field") {
		t.Errorf("strict setter must reject an unmatched column: %v", err)
	}
}

type scanKinds struct {
	Bool      bool
	NamedBool scanNamedBool
	Int       int
	Int8      int8
	Uint      uint
	Uint8     uint8
	Float32   float32
	Float64   float64
	String    string
	Named     scanNamedString
	Time      time.Time
	Null      sql.NullString
	Ptr       *string
	IntPtr    *int
	Slice     []int
	Any       any
}

type scanNamedString string

type scanNamedBool bool

func TestAssignColumnValue(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name    string
		field   string
		value   any
		want    any
		wantErr string
	}{
		{name: "assignable time", field: "Time", value: now, want: now},
		{name: "assignable string", field: "String", value: "s", want: "s"},
		{name: "any field", field: "Any", value: int64(3), want: int64(3)},
		{name: "scanner", field: "Null", value: "s", want: sql.NullString{String: "s", Valid: true}},
		{name: "pointer", field: "Ptr", value: "s", want: "s"},
		{name: "pointer to int", field: "IntPtr", value: int64(4), want: 4},
		{name: "pointer element error", field: "IntPtr", value: "x", wantErr: "invalid syntax"},

		{name: "bool from bool", field: "Bool", value: true, want: true},
		{name: "named bool from bool", field: "NamedBool", value: true, want: scanNamedBool(true)},
		{name: "bool from 1", field: "Bool", value: int64(1), want: true},
		{name: "bool from 0", field: "Bool", value: int64(0), want: false},
		{name: "bool from text", field: "Bool", value: "true", want: true},
		{name: "bool from bad text", field: "Bool", value: "maybe", wantErr: "invalid syntax"},
		{name: "bool from 2", field: "Bool", value: int64(2), wantErr: "cannot assign 2 to a bool"},
		{name: "bool from struct", field: "Bool", value: struct{}{}, wantErr: "cannot assign {} to a bool"},

		{name: "int from int64", field: "Int", value: int64(7), want: 7},
		{name: "int from uint64", field: "Int", value: uint64(7), want: 7},
		{name: "int from huge uint64", field: "Int", value: uint64(math.MaxUint64), wantErr: "overflows int64"},
		{name: "int from whole float", field: "Int", value: float64(7), want: 7},
		{name: "int from fraction", field: "Int", value: 7.5, wantErr: "is not an int64"},
		{name: "int from NaN", field: "Int", value: math.NaN(), wantErr: "is not an int64"},
		{name: "int from infinity", field: "Int", value: math.Inf(1), wantErr: "is not an int64"},
		{name: "int from true", field: "Int", value: true, want: 1},
		{name: "int from false", field: "Int", value: false, want: 0},
		{name: "int from text", field: "Int", value: "42", want: 42},
		{name: "int from bad text", field: "Int", value: "x", wantErr: "invalid syntax"},
		{name: "int8 overflow", field: "Int8", value: int64(300), wantErr: "overflows int8"},

		{name: "uint from uint64", field: "Uint", value: uint64(7), want: uint(7)},
		{name: "uint from int64", field: "Uint", value: int64(7), want: uint(7)},
		{name: "uint from negative", field: "Uint", value: int64(-1), wantErr: "cannot assign -1 to an unsigned integer"},
		{name: "uint from struct", field: "Uint", value: struct{}{}, wantErr: "cannot assign {} to an unsigned integer"},
		{name: "uint from whole float", field: "Uint", value: float64(7), want: uint(7)},
		{name: "uint from negative float", field: "Uint", value: -1.0, wantErr: "is not a uint64"},
		{name: "uint from fraction", field: "Uint", value: 1.5, wantErr: "is not a uint64"},
		{name: "uint from huge float", field: "Uint", value: 1e30, wantErr: "is not a uint64"},
		{name: "uint from text", field: "Uint", value: "42", want: uint(42)},
		{name: "uint from bad text", field: "Uint", value: "-1", wantErr: "invalid syntax"},
		{name: "uint8 overflow", field: "Uint8", value: uint64(300), wantErr: "overflows uint8"},

		{name: "float from float", field: "Float64", value: 1.5, want: 1.5},
		{name: "float from int64", field: "Float64", value: int64(2), want: 2.0},
		{name: "float from uint64", field: "Float64", value: uint64(2), want: 2.0},
		{name: "float from text", field: "Float64", value: "13.86", want: 13.86},
		{name: "float from NaN text", field: "Float64", value: "NaN", want: math.NaN()},
		{name: "float from bad text", field: "Float64", value: "x", wantErr: "invalid syntax"},
		{name: "float from struct", field: "Float64", value: struct{}{}, wantErr: "cannot assign struct {} to a float"},
		{name: "float32 overflow", field: "Float32", value: 1e300, wantErr: "overflows float32"},
		{name: "float32", field: "Float32", value: 1.5, want: float32(1.5)},

		{name: "named string", field: "Named", value: "n", want: scanNamedString("n")},
		{name: "string from int", field: "String", value: int64(1), wantErr: "cannot assign int64 to string"},
		{name: "slice", field: "Slice", value: "x", wantErr: "cannot assign string to []int"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target scanKinds
			field := reflect.ValueOf(&target).Elem().FieldByName(tt.field)
			err := assignColumnValue(field, tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("got error %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := field.Interface()
			if field.Kind() == reflect.Pointer {
				got = field.Elem().Interface()
			}
			if f, ok := tt.want.(float64); ok && math.IsNaN(f) {
				if g, ok := got.(float64); !ok || !math.IsNaN(g) {
					t.Fatalf("got %v, want NaN", got)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %T(%v), want %T(%v)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestAssignColumnValue_NilStoresZero(t *testing.T) {
	target := scanKinds{Int: 5, String: "x"}
	value := "x"
	target.Ptr = &value
	rv := reflect.ValueOf(&target).Elem()
	for _, name := range []string{"Int", "String", "Ptr"} {
		if err := assignColumnValue(rv.FieldByName(name), nil); err != nil {
			t.Fatal(err)
		}
	}
	if target.Int != 0 || target.String != "" || target.Ptr != nil {
		t.Errorf("nil must store the zero value: %+v", target)
	}
}
