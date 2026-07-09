package dalgo2sql

import (
	"testing"
)

func TestPlaceholderDialect_Question(t *testing.T) {
	d := PlaceholderQuestion
	if got := d.placeholder(1); got != "?" {
		t.Errorf("placeholder(1) = %q, want %q", got, "?")
	}
	if got := d.placeholder(5); got != "?" {
		t.Errorf("placeholder(5) = %q, want %q", got, "?")
	}
	sql := "SELECT * FROM t WHERE id = ? AND name = ?"
	if got := d.rewritePlaceholders(sql); got != sql {
		t.Errorf("rewritePlaceholders should be no-op for Question dialect, got %q", got)
	}
}

func TestPlaceholderDialect_Dollar(t *testing.T) {
	d := PlaceholderDollar
	if got := d.placeholder(1); got != "$1" {
		t.Errorf("placeholder(1) = %q, want %q", got, "$1")
	}
	if got := d.placeholder(5); got != "$5" {
		t.Errorf("placeholder(5) = %q, want %q", got, "$5")
	}
	sql := "SELECT * FROM t WHERE id = ? AND name = ?"
	want := "SELECT * FROM t WHERE id = $1 AND name = $2"
	if got := d.rewritePlaceholders(sql); got != want {
		t.Errorf("rewritePlaceholders = %q, want %q", got, want)
	}
}

func TestPlaceholderDialect_DollarNoPlaceholders(t *testing.T) {
	d := PlaceholderDollar
	sql := "SELECT * FROM t"
	if got := d.rewritePlaceholders(sql); got != sql {
		t.Errorf("rewritePlaceholders with no ? should be unchanged, got %q", got)
	}
}
