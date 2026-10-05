package dalgo2sql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Shapes that reflect.VisibleFields hides but scany filled: a promoted field
// whose Go name is taken by a shallower field, and two embedded structs that
// share a Go field name at the same depth. Both are told apart by `db` tags.

type walkUser struct {
	ID string `db:"user_id"`
}

type walkAccount struct {
	ID string `db:"account_id"`
}

type walkTwoEmbedded struct {
	walkUser
	walkAccount
}

type walkLegacy struct {
	ID string `db:"legacy_id"`
}

type walkShadowed struct {
	ID string `db:"id"`
	walkLegacy
}

type walkShallowerUntagged struct {
	Name string
	walkNamed
}

type walkNamed struct {
	Name string
}

type walkEqualDepthUntagged struct {
	walkNamed
	walkOtherNamed
}

type walkOtherNamed struct {
	Name string
}

type walkSkippedEmbedded struct {
	walkUser `db:"-"`
	Name     string
}

// ExportedText is an embedded field that is not a struct: it is a column of its
// own, named after its type. unexportedText is not exported and so is skipped.
type ExportedText string

type unexportedText string

type walkNonStructEmbedded struct {
	ExportedText
	unexportedText
	Name string
}

type walkSelfEmbedded struct {
	*walkSelfEmbedded
	Name string
}

type walkDiamondTop struct {
	walkDiamondLeft
	walkDiamondRight
}

type walkDiamondLeft struct{ walkDiamondBottom }

type walkDiamondRight struct{ walkDiamondBottom }

type walkDiamondBottom struct {
	Depth string
}

// An untagged field declared before a field tagged with the same name in
// another case: the tagged field owns the column `name`, because an exact
// spelling beats a folded one. scany gave `name` to the first field.
type walkUntaggedFirst struct {
	Name  string
	Alias string `db:"name"`
}

// An untagged outer ID with an embedded ID tagged db:"id": the exact spelling `id`
// is the embedded field's, although the outer field is shallower, and the column
// `ID` is the outer field's. The exact spelling beats depth, as it beats a looser
// spelling.
type walkOuterUntaggedID struct {
	ID string
	walkTaggedID
}

type walkTaggedID struct {
	ID string `db:"id"`
}

// A named struct field tagged db:"" is one column, named after the field: scany
// maps the nested struct's fields without a prefix, this package does not.
type walkNamedStructField struct {
	Name string
	Body walkBody `db:""`
}

type walkBody struct {
	Text string
}

func TestStructColumnsOf_AnExactSpellingBeatsDepth(t *testing.T) {
	columns := structColumnsOf(reflect.TypeOf(walkOuterUntaggedID{}))
	tagged, taggedOK := columns.lookup("id")
	outer, outerOK := columns.lookup("ID")
	if !taggedOK || !outerOK {
		t.Fatalf("id found %v, ID found %v", taggedOK, outerOK)
	}
	if !reflect.DeepEqual(tagged.index, []int{1, 0}) || !reflect.DeepEqual(outer.index, []int{0}) {
		t.Errorf("id at %v, ID at %v", tagged.index, outer.index)
	}
}

func TestStructColumnsOf_ANamedStructFieldTaggedEmptyIsNotWalked(t *testing.T) {
	columns := structColumnsOf(reflect.TypeOf(walkNamedStructField{}))
	if _, ok := columns.lookup("Text"); ok {
		t.Error("the fields of a named struct field were reached without a prefix")
	}
	if got, ok := columns.lookup("body"); !ok || !reflect.DeepEqual(got.index, []int{1}) {
		t.Errorf("the named field is one column: %v, %v", got.index, ok)
	}
}

func TestStructColumnsOf_ReachesEveryFieldScanyFilled(t *testing.T) {
	t.Run("two embedded structs with the same Go name and different tags", func(t *testing.T) {
		columns := structColumnsOf(reflect.TypeOf(walkTwoEmbedded{}))
		user, userOK := columns.lookup("user_id")
		account, accountOK := columns.lookup("account_id")
		if !userOK || !accountOK {
			t.Fatalf("user_id found %v, account_id found %v", userOK, accountOK)
		}
		if !reflect.DeepEqual(user.index, []int{0, 0}) || !reflect.DeepEqual(account.index, []int{1, 0}) {
			t.Errorf("user_id at %v, account_id at %v", user.index, account.index)
		}
		if user.id == account.id {
			t.Errorf("distinct fields share the id %d", user.id)
		}
	})
	t.Run("an outer field shadowing an embedded one with a different tag", func(t *testing.T) {
		columns := structColumnsOf(reflect.TypeOf(walkShadowed{}))
		outer, outerOK := columns.lookup("id")
		legacy, legacyOK := columns.lookup("legacy_id")
		if !outerOK || !legacyOK {
			t.Fatalf("id found %v, legacy_id found %v", outerOK, legacyOK)
		}
		if !reflect.DeepEqual(outer.index, []int{0}) || !reflect.DeepEqual(legacy.index, []int{1, 0}) {
			t.Errorf("id at %v, legacy_id at %v", outer.index, legacy.index)
		}
	})
	t.Run("untagged same name, the shallower field wins", func(t *testing.T) {
		got, ok := structColumnsOf(reflect.TypeOf(walkShallowerUntagged{})).lookup("name")
		if !ok || !reflect.DeepEqual(got.index, []int{0}) {
			t.Errorf("got %v, %v", got.index, ok)
		}
	})
	t.Run("untagged same name at one depth, the first wins", func(t *testing.T) {
		got, ok := structColumnsOf(reflect.TypeOf(walkEqualDepthUntagged{})).lookup("name")
		if !ok || !reflect.DeepEqual(got.index, []int{0, 0}) {
			t.Errorf("got %v, %v", got.index, ok)
		}
	})
	t.Run("an exact tagged name beats an untagged field declared before it", func(t *testing.T) {
		columns := structColumnsOf(reflect.TypeOf(walkUntaggedFirst{}))
		if got, ok := columns.lookup("name"); !ok || !reflect.DeepEqual(got.index, []int{1}) {
			t.Errorf("name: %v, %v", got.index, ok)
		}
		if got, ok := columns.lookup("Name"); !ok || !reflect.DeepEqual(got.index, []int{0}) {
			t.Errorf("Name: %v, %v", got.index, ok)
		}
	})
	t.Run("an embedded struct tagged db:\"-\" is skipped with all its fields", func(t *testing.T) {
		columns := structColumnsOf(reflect.TypeOf(walkSkippedEmbedded{}))
		if _, ok := columns.lookup("user_id"); ok {
			t.Error("user_id of a skipped embedded struct must not match")
		}
		if _, ok := columns.lookup("ID"); ok {
			t.Error("ID of a skipped embedded struct must not match")
		}
		if _, ok := columns.lookup("name"); !ok {
			t.Error("Name beside a skipped embedded struct must match")
		}
	})
	t.Run("an embedded field that is not a struct is a column if exported", func(t *testing.T) {
		columns := structColumnsOf(reflect.TypeOf(walkNonStructEmbedded{}))
		if _, ok := columns.lookup("exportedtext"); !ok {
			t.Error("ExportedText must match")
		}
		if _, ok := columns.lookup("unexportedtext"); ok {
			t.Error("unexportedText must not match")
		}
	})
	t.Run("a self-embedding pointer does not loop", func(t *testing.T) {
		got, ok := structColumnsOf(reflect.TypeOf(walkSelfEmbedded{})).lookup("name")
		if !ok || !reflect.DeepEqual(got.index, []int{1}) {
			t.Errorf("got %v, %v", got.index, ok)
		}
	})
	t.Run("a type reached by two paths is walked once", func(t *testing.T) {
		columns := structColumnsOf(reflect.TypeOf(walkDiamondTop{}))
		got, ok := columns.lookup("depth")
		if !ok || !reflect.DeepEqual(got.index, []int{0, 0, 0}) {
			t.Errorf("got %v, %v", got.index, ok)
		}
		if len(columns.exact) != 1 {
			t.Errorf("one field expected, got %v", columns.exact)
		}
	})
}

func TestScanIntoData_StructPromotedFieldsThatShareAGoName(t *testing.T) {
	t.Run("two embedded structs", func(t *testing.T) {
		var got walkTwoEmbedded
		if err := getScanned(t, &got, []string{"user_id", "account_id"}, "u", "a"); err != nil {
			t.Fatal(err)
		}
		if got.walkUser.ID != "u" || got.walkAccount.ID != "a" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("an outer field and an embedded one", func(t *testing.T) {
		var got walkShadowed
		if err := getScanned(t, &got, []string{"id", "legacy_id"}, "i", "l"); err != nil {
			t.Fatal(err)
		}
		if got.ID != "i" || got.walkLegacy.ID != "l" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("untagged same name, the shallower field gets the column", func(t *testing.T) {
		var got walkShallowerUntagged
		if err := getScanned(t, &got, []string{"name"}, "n"); err != nil {
			t.Fatal(err)
		}
		if got.Name != "n" || got.walkNamed.Name != "" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("an embedded struct tagged db:\"-\" is not filled", func(t *testing.T) {
		var got walkSkippedEmbedded
		err := getScanned(t, &got, []string{"name", "user_id"}, "n", "u")
		if err == nil || !strings.Contains(err.Error(), `column "user_id": no corresponding field`) {
			t.Errorf("got %v", err)
		}
	})
	t.Run("a tagged field owns its exact column, an untagged one declared first keeps its Go name", func(t *testing.T) {
		var got walkUntaggedFirst
		if err := getScanned(t, &got, []string{"name", "Name"}, "tagged", "untagged"); err != nil {
			t.Fatal(err)
		}
		if got.Alias != "tagged" || got.Name != "untagged" {
			t.Errorf("got %+v", got)
		}
	})
}

func TestScanIntoData_StructEmbeddedFieldsThatAreNotStructs(t *testing.T) {
	got := walkNonStructEmbedded{unexportedText: "kept"}
	if err := getScanned(t, &got, []string{"exportedtext", "name"}, "e", "n"); err != nil {
		t.Fatal(err)
	}
	if got.ExportedText != "e" || got.Name != "n" || got.unexportedText != "kept" {
		t.Errorf("got %+v", got)
	}
	err := getScanned(t, &got, []string{"unexportedtext"}, "x")
	if err == nil || !strings.Contains(err.Error(), `column "unexportedtext": no corresponding field`) {
		t.Errorf("got %v", err)
	}
	if got.unexportedText != "kept" {
		t.Errorf("an unexported embedded field is never written: %+v", got)
	}
}

func TestScanIntoData_StructSelfEmbeddedPointerIsNotWalkedAgain(t *testing.T) {
	inner := &walkSelfEmbedded{Name: "inner"}
	got := walkSelfEmbedded{walkSelfEmbedded: inner, Name: "outer"}
	if err := getScanned(t, &got, []string{"name"}, "n"); err != nil {
		t.Fatal(err)
	}
	if got.Name != "n" || inner.Name != "inner" {
		t.Errorf("the outer field takes the column: %+v, inner %+v", got, inner)
	}
}

func TestRecordsReader_StructTargetAnExactSpellingBeatsDepth(t *testing.T) {
	var got walkOuterUntaggedID
	if err := nextStruct(t, &got, sqlmock.NewRows([]string{"id", "ID"}).AddRow("tagged", "outer")); err != nil {
		t.Fatal(err)
	}
	if got.walkTaggedID.ID != "tagged" || got.ID != "outer" {
		t.Errorf("got %+v", got)
	}
	// With no column ID, the outer field stays empty: `id` is not given to it.
	got = walkOuterUntaggedID{}
	if err := nextStruct(t, &got, sqlmock.NewRows([]string{"id"}).AddRow("tagged")); err != nil {
		t.Fatal(err)
	}
	if got.walkTaggedID.ID != "tagged" || got.ID != "" {
		t.Errorf("got %+v", got)
	}
}

func TestRecordsReader_StructTargetPromotedFieldsThatShareAGoName(t *testing.T) {
	t.Run("two embedded structs", func(t *testing.T) {
		var got walkTwoEmbedded
		if err := nextStruct(t, &got, sqlmock.NewRows([]string{"user_id", "account_id"}).AddRow("u", "a")); err != nil {
			t.Fatal(err)
		}
		if got.walkUser.ID != "u" || got.walkAccount.ID != "a" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("an outer field and an embedded one", func(t *testing.T) {
		var got walkShadowed
		if err := nextStruct(t, &got, sqlmock.NewRows([]string{"id", "legacy_id"}).AddRow("i", "l")); err != nil {
			t.Fatal(err)
		}
		if got.ID != "i" || got.walkLegacy.ID != "l" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("untagged same name, the shallower field gets the column", func(t *testing.T) {
		var got walkShallowerUntagged
		if err := nextStruct(t, &got, sqlmock.NewRows([]string{"name"}).AddRow("n")); err != nil {
			t.Fatal(err)
		}
		if got.Name != "n" || got.walkNamed.Name != "" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("an embedded struct tagged db:\"-\" is skipped", func(t *testing.T) {
		var got walkSkippedEmbedded
		if err := nextStruct(t, &got, sqlmock.NewRows([]string{"name", "user_id"}).AddRow("n", "u")); err != nil {
			t.Fatal(err)
		}
		if got.Name != "n" || got.ID != "" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("a tagged field owns its exact column, an untagged one declared first keeps its Go name", func(t *testing.T) {
		var got walkUntaggedFirst
		if err := nextStruct(t, &got, sqlmock.NewRows([]string{"name", "Name"}).AddRow("tagged", "untagged")); err != nil {
			t.Fatal(err)
		}
		if got.Alias != "tagged" || got.Name != "untagged" {
			t.Errorf("got %+v", got)
		}
	})
}
