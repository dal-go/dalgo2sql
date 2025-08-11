package dalgo2sql

import (
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// Types for testing SetID detection

type withSetIDValue struct{}

func (withSetIDValue) SetID(id any) {}

type withSetIDPtr struct{}

func (*withSetIDPtr) SetID(id any) {}

// Types for field presence

type hasExportedID struct {
	ID any
}

type hasUnexportedID struct {
	id any
}

// Types for DataToKey (GetID)

type getIDVal struct{}

func (getIDVal) GetID() any { return int64(101) }

type getIDPtr struct{}

func (*getIDPtr) GetID() any { return "u-abc" }

// helper to collect fields into a map for easy assertions
func toMap(fields []dal.ExtraField) map[string]any {
	m := make(map[string]any, len(fields))
	for _, f := range fields {
		m[f.Name()] = f.Value()
	}
	return m
}

func Test_simpleKeyToFields_DataNil_NoParents(t *testing.T) {
	f := simpleKeyToFields("ID")
	key := dal.NewKeyWithID("users", 123)
	fields, err := f(key, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	m := toMap(fields)
	if got := m["ID"]; got != 123 {
		t.Fatalf("expected ID=123, got %v", got)
	}
}

func Test_simpleKeyToFields_DataNil_WithParents(t *testing.T) {
	f := simpleKeyToFields("ID")
	root := dal.NewKeyWithID("orgs", "acme")
	child := dal.NewKeyWithParentAndID(root, "users", 7)
	fields, err := f(child, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields (parent + own ID), got %d", len(fields))
	}
	m := toMap(fields)
	if got := m["ID"]; got != 7 {
		t.Fatalf("expected ID=7, got %v", got)
	}
	if got := m["orgs_acme"]; got != "acme" {
		t.Fatalf("expected parent field orgs_acme=acme, got %v", got)
	}
}

func Test_simpleKeyToFields_SetID_ValueReceiver_ParentsIncluded(t *testing.T) {
	f := simpleKeyToFields("ID")
	p := dal.NewKeyWithID("teams", "t1")
	k := dal.NewKeyWithParentAndID(p, "users", 1)
	data := withSetIDValue{}
	fields, err := f(k, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// own ID should be omitted, but parent field included
	if len(fields) != 1 {
		t.Fatalf("expected only parent field, got %d", len(fields))
	}
	m := toMap(fields)
	if got, ok := m["teams_t1"]; !ok || got != "t1" {
		t.Fatalf("expected parent field teams_t1=t1, got %v (present=%v)", got, ok)
	}
}

func Test_simpleKeyToFields_SetID_PtrReceiver_ParentsIncluded(t *testing.T) {
	f := simpleKeyToFields("ID")
	p := dal.NewKeyWithID("teams", "t1")
	k := dal.NewKeyWithParentAndID(p, "users", 1)
	data := &withSetIDPtr{}
	fields, err := f(k, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("expected only parent field, got %d", len(fields))
	}
	if fields[0].Name() != "teams_t1" || fields[0].Value() != "t1" {
		t.Fatalf("unexpected parent field: %s=%v", fields[0].Name(), fields[0].Value())
	}
}

func Test_simpleKeyToFields_ExportedIDField_ParentsIncluded(t *testing.T) {
	f := simpleKeyToFields("ID")
	p := dal.NewKeyWithID("departments", 42)
	k := dal.NewKeyWithParentAndID(p, "users", 99)
	data := hasExportedID{}
	fields, err := f(k, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("expected only one parent field, got %d", len(fields))
	}
	m := toMap(fields)
	if got := m["departments_42"]; got != 42 {
		t.Fatalf("expected parent field departments_42=42, got %v", got)
	}
}

func Test_simpleKeyToFields_UnexportedIDField_AddOwnID(t *testing.T) {
	f := simpleKeyToFields("ID")
	k := dal.NewKeyWithID("users", 55)
	data := hasUnexportedID{}
	fields, err := f(k, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	if fields[0].Name() != "ID" || fields[0].Value() != 55 {
		t.Fatalf("unexpected field: %s=%v", fields[0].Name(), fields[0].Value())
	}
}

func Test_simpleKeyToFields_EmptyFieldName_Computed_IncludesParents(t *testing.T) {
	f := simpleKeyToFields("")
	grand := dal.NewKeyWithID("companies", "globex")
	parent := dal.NewKeyWithParentAndID(grand, "projects", 777)
	k := dal.NewKeyWithParentAndID(parent, "tasks", "a1")
	data := struct{}{}
	fields, err := f(k, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// data has no SetID and no exported field with computed name -> should include own ID plus two parents
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields (2 parents + own id), got %d", len(fields))
	}
	m := toMap(fields)
	// own field name should be tasks_a1 (collection_tasks + _ + id)
	if got, ok := m["tasks_a1"]; !ok || !reflect.DeepEqual(got, "a1") {
		t.Fatalf("expected own field tasks_a1='a1', got %v (present=%v)", got, ok)
	}
	if got := m["projects_777"]; got != 777 {
		t.Fatalf("expected parent field projects_777=777, got %v", got)
	}
	if got := m["companies_globex"]; got != "globex" {
		t.Fatalf("expected grandparent field companies_globex='globex', got %v", got)
	}
}

// --------- Tests for simpleFieldsToKey ---------

func Test_simpleFieldsToKey_GetID_ValueReceiver_Int64Kind(t *testing.T) {
	schema := NewSimpleSchema("ID")
	parent := dal.NewKeyWithID("orgs", "acme")
	incomplete := dal.NewIncompleteKey("users", reflect.Int64, parent)
	data := getIDVal{}
	key, err := schema.DataToKey(incomplete, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.Collection() != "users" || key.Parent() == nil || key.Parent().Collection() != "orgs" {
		t.Fatalf("parent/collection mismatch in key: %v", key)
	}
	if kind := reflect.TypeOf(key.ID).Kind(); kind != reflect.Int64 {
		t.Fatalf("expected id kind int64, got %v", kind)
	}
	if key.ID != int64(101) {
		t.Fatalf("expected id 101, got %v", key.ID)
	}
}

func Test_simpleFieldsToKey_GetID_PtrReceiver_StringKind(t *testing.T) {
	schema := NewSimpleSchema("ID")
	parent := dal.NewKeyWithID("orgs", "acme")
	incomplete := dal.NewIncompleteKey("users", reflect.String, parent)
	data := &getIDPtr{}
	key, err := schema.DataToKey(incomplete, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.ID != "u-abc" {
		t.Fatalf("expected id 'u-abc', got %v", key.ID)
	}
	if key.Parent() == nil || key.Parent().Collection() != "orgs" {
		t.Fatalf("expected parent preserved")
	}
}

func Test_simpleFieldsToKey_FieldByName_Custom(t *testing.T) {
	schema := NewSimpleSchema("UserID")
	parent := dal.NewKeyWithID("orgs", "acme")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, parent)
	data := struct{ UserID int }{UserID: 7}
	key, err := schema.DataToKey(incomplete, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.ID != 7 {
		t.Fatalf("expected id 7, got %v", key.ID)
	}
}

func Test_simpleFieldsToKey_FieldByName_DefaultID_WhenEmptyName(t *testing.T) {
	schema := NewSimpleSchema("")
	parent := dal.NewKeyWithID("orgs", "acme")
	incomplete := dal.NewIncompleteKey("users", reflect.Uint32, parent)
	data := struct{ ID uint32 }{ID: 42}
	key, err := schema.DataToKey(incomplete, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.ID != uint32(42) {
		t.Fatalf("expected id 42, got %v", key.ID)
	}
}

func Test_simpleFieldsToKey_FieldByName_OnPointer(t *testing.T) {
	schema := NewSimpleSchema("UserID")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, nil)
	ptr := &struct{ UserID int }{UserID: 8}
	key, err := schema.DataToKey(incomplete, ptr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.ID != 8 {
		t.Fatalf("expected id 8, got %v", key.ID)
	}
}

func Test_simpleFieldsToKey_TypeConversion(t *testing.T) {
	schema := NewSimpleSchema("ID")
	incomplete := dal.NewIncompleteKey("things", reflect.Int64, nil)
	data := struct{ ID int32 }{ID: 9}
	key, err := schema.DataToKey(incomplete, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind := reflect.TypeOf(key.ID).Kind(); kind != reflect.Int64 {
		t.Fatalf("expected converted id kind int64, got %v", kind)
	}
	if key.ID != int64(9) {
		t.Fatalf("expected id 9, got %v", key.ID)
	}
}

func Test_simpleFieldsToKey_ErrorWhenMissingID(t *testing.T) {
	schema := NewSimpleSchema("UserID")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, nil)
	data := struct{ Name string }{Name: "x"}
	_, err := schema.DataToKey(incomplete, data)
	if err == nil {
		t.Fatalf("expected error for missing id field/method, got nil")
	}
}

func Test_simpleFieldsToKey_ErrorOnUnexportedField(t *testing.T) {
	schema := NewSimpleSchema("id")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, nil)
	data := struct{ id int }{id: 1}
	_, err := schema.DataToKey(incomplete, data)
	if err == nil {
		t.Fatalf("expected error for unexported field, got nil")
	}
}

func Test_simpleFieldsToKey_ErrorOnNonStructData(t *testing.T) {
	schema := NewSimpleSchema("ID")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, nil)
	var data any = 5
	_, err := schema.DataToKey(incomplete, data)
	if err == nil {
		t.Fatalf("expected error for non-struct data, got nil")
	}
}

func Test_simpleFieldsToKey_ErrorOnConvertFailure(t *testing.T) {
	schema := NewSimpleSchema("ID")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, nil)
	data := struct{ ID string }{ID: "abc"}
	_, err := schema.DataToKey(incomplete, data)
	if err == nil {
		t.Fatalf("expected error on convert failure, got nil")
	}
}

func Test_simpleFieldsToKey_ErrorOnNilData(t *testing.T) {
	schema := NewSimpleSchema("ID")
	incomplete := dal.NewIncompleteKey("users", reflect.Int, nil)
	_, err := schema.DataToKey(incomplete, nil)
	if err == nil {
		t.Fatalf("expected error for nil data, got nil")
	}
}
