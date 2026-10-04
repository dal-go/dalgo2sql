package dalgo2sql

import (
	"errors"
	"slices"
	"strings"
	"testing"

	dalrecord "github.com/dal-go/record"
)

func TestSQLIdentifier_WithoutDialect(t *testing.T) {
	options := DbOptions{}

	t.Run("a plain identifier is written exactly as given", func(t *testing.T) {
		for _, name := range []string{"users", "Users", "_x", "a1", "Order_Details", "A", "_"} {
			got, err := options.sqlIdentifier(positionCollection, name)
			if err != nil || got != name {
				t.Errorf("sqlIdentifier(%q) = %q, %v; want the name unchanged", name, got, err)
			}
		}
	})

	t.Run("a name that is not a plain identifier is refused", func(t *testing.T) {
		refused := append([]string{"", "Order Details", "1abc", "a-b", "a.b", "a$b", "é", "naïve", "ａ", "a\n", "\na"}, hostileNames...)
		for _, name := range refused {
			got, err := options.sqlIdentifier(positionField, name)
			if got != "" || !errors.Is(err, ErrUnsafeName) {
				t.Errorf("sqlIdentifier(%q) = %q, %v; want a refusal wrapping ErrUnsafeName", name, got, err)
			}
		}
	})

	// PostgreSQL truncates a longer identifier to 63 bytes, so a name past the
	// bound would address the table named by its first bytes.
	t.Run("a plain identifier is bounded like a quoted name", func(t *testing.T) {
		atBound := strings.Repeat("a", maxNameBytes)
		if got, err := options.sqlIdentifier(positionCollection, atBound); err != nil || got != atBound {
			t.Errorf("a plain identifier of %d bytes was refused: %v", maxNameBytes, err)
		}
		_, err := options.sqlIdentifier(positionCollection, atBound+"a")
		if !errors.Is(err, ErrUnsafeName) || !strings.Contains(err.Error(), "is too long") {
			t.Errorf("a plain identifier of %d bytes: error = %v, want a refusal that says it is too long", maxNameBytes+1, err)
		}
		if err != nil && len(err.Error()) > 200 {
			t.Errorf("the refusal is %d bytes long, it must not echo the name", len(err.Error()))
		}
	})

	// A name carrying its own quotes used to act as a quoted identifier, because
	// names were pasted as written. Without a dialect there is no reviewed quoting
	// to write it with, so it is refused.
	t.Run("a name that carries its own quoting is refused", func(t *testing.T) {
		for _, dialect := range []string{"", "postgres", "mysql"} {
			options := DbOptions{StructuredQueryDialect: dialect}
			for _, name := range selfQuotedNames {
				if got, err := options.sqlIdentifier(positionCollection, name); got != "" || !errors.Is(err, ErrUnsafeName) {
					t.Errorf("dialect %q: sqlIdentifier(%q) = %q, %v; want a refusal wrapping ErrUnsafeName", dialect, name, got, err)
				}
			}
		}
	})

	t.Run("a dialect without reviewed quoting is treated as no dialect", func(t *testing.T) {
		for _, dialect := range []string{"postgres", "mysql", "example-native", "SQLite", " sqlite"} {
			options := DbOptions{StructuredQueryDialect: dialect}
			if got, err := options.sqlIdentifier(positionCollection, "users"); err != nil || got != "users" {
				t.Errorf("dialect %q: sqlIdentifier(users) = %q, %v", dialect, got, err)
			}
			if _, err := options.sqlIdentifier(positionCollection, "Order Details"); !errors.Is(err, ErrUnsafeName) {
				t.Errorf("dialect %q: a name with a space was not refused: %v", dialect, err)
			}
		}
	})
}

func TestSQLIdentifier_SQLite(t *testing.T) {
	options := DbOptions{StructuredQueryDialect: "sqlite"}

	t.Run("a name is quoted and its quote character escaped", func(t *testing.T) {
		quoted := map[string]string{
			"users":                             "`users`",
			"Order Details":                     "`Order Details`",
			"Unit Price":                        "`Unit Price`",
			"a`b":                               "`a``b`",
			"``":                                "``````",
			`a" OR 1=1 --`:                      "`a\" OR 1=1 --`",
			"x; DROP TABLE y":                   "`x; DROP TABLE y`",
			"it's":                              "`it's`",
			"a--b":                              "`a--b`",
			"a/*b*/c":                           "`a/*b*/c`",
			"café":                              "`café`",
			"1abc":                              "`1abc`",
			"a.b":                               "`a.b`",
			"a?b":                               "`a?b`",
			strings.Repeat("é", maxNameBytes/2): "`" + strings.Repeat("é", maxNameBytes/2) + "`",
			strings.Repeat("a", maxNameBytes):   "`" + strings.Repeat("a", maxNameBytes) + "`",
		}
		for name, want := range quoted {
			got, err := options.sqlIdentifier(positionField, name)
			if err != nil || got != want {
				t.Errorf("sqlIdentifier(%q) = %q, %v; want %q", name, got, err, want)
			}
		}
	})

	// The name is one literal identifier, quote characters included, so a caller
	// that used to pass `"Order Details"` to reach the table Order Details must
	// pass Order Details now.
	t.Run("a name that carries its own quoting is one literal name", func(t *testing.T) {
		want := map[string]string{
			`"Order Details"`: "`\"Order Details\"`",
			"[Order Details]": "`[Order Details]`",
			"`x`":             "```x```",
			"'x'":             "`'x'`",
		}
		if len(want) != len(selfQuotedNames) {
			t.Fatalf("%d expectations for %d names", len(want), len(selfQuotedNames))
		}
		for name, quoted := range want {
			if got, err := options.sqlIdentifier(positionCollection, name); err != nil || got != quoted {
				t.Errorf("sqlIdentifier(%q) = %q, %v; want %q", name, got, err, quoted)
			}
		}
	})

	t.Run("a name that cannot be quoted is refused", func(t *testing.T) {
		for _, name := range append([]string{""}, unquotableNames...) {
			got, err := options.sqlIdentifier(positionPrimaryKey, name)
			if got != "" || !errors.Is(err, ErrUnsafeName) {
				t.Errorf("sqlIdentifier(%q) = %q, %v; want a refusal wrapping ErrUnsafeName", name, got, err)
			}
		}
	})

	t.Run("each refusal gives its reason and never the offending bytes raw", func(t *testing.T) {
		reasons := map[string]string{
			"":                       "is empty",
			"a\x00b":                 "contains a control character",
			"a\x7fb":                 "contains a control character",
			"bad\xffutf8":            "is not valid UTF-8",
			strings.Repeat("a", 256): "is too long",
		}
		for name, reason := range reasons {
			_, err := options.sqlIdentifier(positionCollection, name)
			if err == nil || !strings.Contains(err.Error(), reason) {
				t.Errorf("sqlIdentifier(%q): error %v does not say %q", name, err, reason)
			}
			if err != nil && holdsRawByte(err.Error(), 0x00, 0x7f, 0xff) {
				t.Errorf("sqlIdentifier(%q): error %q holds a raw control or invalid byte", name, err)
			}
		}
	})
}

// holdsRawByte reports whether s holds one of the given bytes as is.
func holdsRawByte(s string, bytes ...byte) bool {
	for i := 0; i < len(s); i++ {
		if slices.Contains(bytes, s[i]) {
			return true
		}
	}
	return false
}

func TestUnsafeNameError(t *testing.T) {
	options := DbOptions{}

	t.Run("names the position and the name", func(t *testing.T) {
		for _, position := range []string{positionCollection, positionField, positionPrimaryKey} {
			_, err := options.sqlIdentifier(position, "x; DROP TABLE y")
			want := `unsafe SQL name: ` + position + ` name "x; DROP TABLE y" is not a plain identifier`
			if err == nil || err.Error() != want {
				t.Errorf("error = %v, want %q", err, want)
			}
			var unsafe *unsafeNameError
			if !errors.As(err, &unsafe) || unsafe.position != position {
				t.Errorf("error = %#v, want an *unsafeNameError for %q", err, position)
			}
		}
	})

	t.Run("never echoes more than the name, truncated", func(t *testing.T) {
		hostile := strings.Repeat("x", 100) + "; DROP TABLE y"
		_, err := options.sqlIdentifier(positionCollection, hostile)
		if err == nil {
			t.Fatal("a hostile name was accepted")
		}
		if strings.Contains(err.Error(), hostile) || strings.Contains(err.Error(), "DROP") {
			t.Errorf("error %q echoes more than the truncated name", err)
		}
		if want := `collection name "` + strings.Repeat("x", maxNameInError) + `" (truncated)`; !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not hold %q", err, want)
		}
		if len(err.Error()) > 200 {
			t.Errorf("error is %d bytes long", len(err.Error()))
		}
	})

	t.Run("truncates by character, not by byte", func(t *testing.T) {
		name := strings.Repeat("é", maxNameInError) + "é"
		_, err := options.sqlIdentifier(positionField, name)
		if want := `"` + strings.Repeat("é", maxNameInError) + `" (truncated)`; err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to hold %q", err, want)
		}
		_, err = options.sqlIdentifier(positionField, strings.Repeat("é", maxNameInError))
		if err == nil || strings.Contains(err.Error(), "truncated") {
			t.Errorf("a name of exactly %d characters was reported truncated: %v", maxNameInError, err)
		}
	})

	t.Run("escapes a NUL and an invalid byte", func(t *testing.T) {
		_, err := options.sqlIdentifier(positionField, "a\x00b\xff")
		if err == nil || !strings.Contains(err.Error(), `"a\x00b\xff"`) {
			t.Errorf("error = %v", err)
		}
	})
}

func TestRecordsetIdentifier(t *testing.T) {
	orders := dalrecord.NewKeyWithID("orders", "o1")
	lines := dalrecord.NewKeyWithParentAndID(orders, "lines", "l1")

	t.Run("a root key gives its collection", func(t *testing.T) {
		if got, err := (DbOptions{}).recordsetIdentifier(orders); err != nil || got != "orders" {
			t.Errorf("= %q, %v", got, err)
		}
	})

	t.Run("a nested key gives the collections joined as getRecordsetName does", func(t *testing.T) {
		got, err := (DbOptions{}).recordsetIdentifier(lines)
		if err != nil || got != getRecordsetName(lines) || got != "lines_orders" {
			t.Errorf("= %q, %v; want %q", got, err, "lines_orders")
		}
	})

	t.Run("every segment is validated, not only the joined name", func(t *testing.T) {
		for _, hostile := range []string{"x; DROP TABLE y", "a b", "a`b"} {
			parent := dalrecord.NewKeyWithID(hostile, "p1")
			child := dalrecord.NewKeyWithParentAndID(parent, "lines", "l1")
			_, err := (DbOptions{}).recordsetIdentifier(child)
			if !errors.Is(err, ErrUnsafeName) || !strings.Contains(err.Error(), "collection") {
				t.Errorf("hostile parent %q: error = %v", hostile, err)
			}
		}
		// In a dialect that quotes, a segment is judged on its own: a NUL in
		// the parent is refused although the joined name is otherwise fine.
		parent := dalrecord.NewKeyWithID("nul\x00parent", "p1")
		child := dalrecord.NewKeyWithParentAndID(parent, "lines", "l1")
		if _, err := (DbOptions{StructuredQueryDialect: "sqlite"}).recordsetIdentifier(child); !errors.Is(err, ErrUnsafeName) {
			t.Errorf("a NUL in a parent segment was not refused: %v", err)
		}
	})

	// Each case below joins into a name that passes the joined-name check in its
	// mode, so only the check of the segment itself can refuse it.
	t.Run("a segment is refused although the joined name is acceptable", func(t *testing.T) {
		cases := map[string]struct {
			parentCollection string // "" builds a parent with an empty collection
			dialect          string
			joined           string
		}{
			"a leading digit, joined name plain":            {"1abc", "", "lines_1abc"},
			"an empty parent collection, joined name plain": {"", "", "lines_"},
			"an empty parent collection, sqlite":            {"", "sqlite", "`lines_`"},
		}
		for name, tt := range cases {
			t.Run(name, func(t *testing.T) {
				options := DbOptions{StructuredQueryDialect: tt.dialect}
				parent := &dalrecord.Key{ID: "p1"}
				if tt.parentCollection != "" {
					parent = dalrecord.NewKeyWithID(tt.parentCollection, "p1")
				}
				child := dalrecord.NewKeyWithParentAndID(parent, "lines", "l1")
				// The joined name alone is acceptable: that is what makes the case.
				if got, err := options.sqlIdentifier(positionCollection, getRecordsetName(child)); err != nil || got != tt.joined {
					t.Fatalf("joined name: sqlIdentifier = %q, %v; want %q", got, err, tt.joined)
				}
				_, err := options.recordsetIdentifier(child)
				if !errors.Is(err, ErrUnsafeName) || !strings.Contains(err.Error(), positionCollection) {
					t.Errorf("recordsetIdentifier = %v, want a refusal of the segment", err)
				}
			})
		}
	})

	t.Run("a dialect that quotes quotes the joined name once", func(t *testing.T) {
		parent := dalrecord.NewKeyWithID("Order Details", "o1")
		child := dalrecord.NewKeyWithParentAndID(parent, "Line Items", "l1")
		got, err := (DbOptions{StructuredQueryDialect: "sqlite"}).recordsetIdentifier(child)
		if err != nil || got != "`Line Items_Order Details`" {
			t.Errorf("= %q, %v", got, err)
		}
	})

	t.Run("the joined name is bounded too", func(t *testing.T) {
		long := strings.Repeat("a", maxNameBytes-10)
		parent := dalrecord.NewKeyWithID(long, "p1")
		child := dalrecord.NewKeyWithParentAndID(parent, long, "c1")
		if _, err := (DbOptions{StructuredQueryDialect: "sqlite"}).recordsetIdentifier(child); !errors.Is(err, ErrUnsafeName) {
			t.Errorf("a joined name over the bound was not refused: %v", err)
		}
	})
}

func TestRecordFieldNames(t *testing.T) {
	type row struct {
		Name string
		Age  int
	}
	var nilMap map[string]any
	tests := map[string]struct {
		data any
		want []string
	}{
		"struct":             {row{}, []string{"Name", "Age"}},
		"pointer to struct":  {&row{}, []string{"Name", "Age"}},
		"map":                {map[string]any{"only": 1}, []string{"only"}},
		"pointer to map":     {&map[string]any{"only": 1}, []string{"only"}},
		"nil map":            {nilMap, nil},
		"map, non-string":    {map[int]any{1: "x"}, nil},
		"unsupported kind":   {42, nil},
		"nil":                {nil, nil},
		"nil pointer":        {(*row)(nil), nil},
		"empty struct":       {struct{}{}, nil},
		"map, several names": {map[string]int{"b": 1}, []string{"b"}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := recordFieldNames(tt.data)
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("recordFieldNames = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectList(t *testing.T) {
	plain := DbOptions{}
	sqlite := DbOptions{StructuredQueryDialect: "sqlite"}

	t.Run("a wildcard is not a name", func(t *testing.T) {
		for _, options := range []DbOptions{plain, sqlite} {
			if got, err := selectList(options, []string{"*"}, true); err != nil || got != "*" {
				t.Errorf("= %q, %v", got, err)
			}
		}
	})

	t.Run("no fields give an empty list", func(t *testing.T) {
		if got, err := selectList(plain, nil, false); err != nil || got != "" {
			t.Errorf("= %q, %v", got, err)
		}
	})

	t.Run("names are rendered in order", func(t *testing.T) {
		if got, err := selectList(plain, []string{"id", "Name"}, true); err != nil || got != "id, Name" {
			t.Errorf("= %q, %v", got, err)
		}
		if got, err := selectList(sqlite, []string{"Order ID", "Unit Price"}, true); err != nil || got != "`Order ID`, `Unit Price`" {
			t.Errorf("= %q, %v", got, err)
		}
	})

	t.Run("the first name is a primary key only when asked", func(t *testing.T) {
		_, err := selectList(plain, []string{"a b", "Name"}, true)
		if err == nil || !strings.Contains(err.Error(), positionPrimaryKey) {
			t.Errorf("error = %v, want the primary key position", err)
		}
		_, err = selectList(plain, []string{"a b", "Name"}, false)
		if err == nil || !strings.Contains(err.Error(), positionField) {
			t.Errorf("error = %v, want the field position", err)
		}
		_, err = selectList(plain, []string{"id", "a b"}, true)
		if err == nil || !strings.Contains(err.Error(), positionField) {
			t.Errorf("error = %v, want the field position", err)
		}
	})
}
