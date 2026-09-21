package dalgo2sql

import (
	"errors"
	"fmt"

	"github.com/dal-go/dalgo/dal"
)

// translateNativeJoinHints calls an optional trusted adapter translator for a
// complete validated relation tree. A configured translator must explicitly
// acknowledge every edge that carries algorithm preferences so no preference is
// silently lost between DTQL and the native compiler.
func translateNativeJoinHints(from dal.FromSource, translator NativeJoinHintTranslator) (NativeJoinHintFragments, error) {
	if translator == nil || from == nil {
		return NativeJoinHintFragments{}, nil
	}
	if err := dal.ValidateJoinTree(from); err != nil {
		return NativeJoinHintFragments{}, err
	}
	expected := make(map[string]struct{})
	var expectedPaths []string
	var walk func(dal.FromSource, string)
	walk = func(node dal.FromSource, path string) {
		if node == nil {
			return
		}
		for i, join := range node.Joins() {
			joinPath := fmt.Sprintf("%s.joins[%d]", path, i)
			if len(join.Algorithms()) != 0 {
				expected[joinPath] = struct{}{}
				expectedPaths = append(expectedPaths, joinPath)
			}
			child := join.From()
			if child == nil && join.RecordsetSource != nil {
				child = dal.From(join.RecordsetSource)
			}
			walk(child, joinPath+".from")
		}
	}
	walk(from, "from")
	if len(expected) == 0 {
		return NativeJoinHintFragments{}, nil
	}
	fragments, err := translator.TranslateNativeJoinHints(from)
	if err != nil {
		var diagnostic *dal.JoinValidationError
		if errors.As(err, &diagnostic) {
			return NativeJoinHintFragments{}, err
		}
		return NativeJoinHintFragments{}, nativeJoinHintPlanError("from", fmt.Sprintf("native JOIN hint translator: %v", err))
	}
	handled := make(map[string]struct{}, len(fragments.HandledPaths))
	for _, path := range fragments.HandledPaths {
		if _, exists := expected[path]; !exists {
			return NativeJoinHintFragments{}, nativeJoinHintPlanError(path, "native JOIN hint translator acknowledged an unknown path")
		}
		if _, duplicate := handled[path]; duplicate {
			return NativeJoinHintFragments{}, nativeJoinHintPlanError(path, "native JOIN hint translator acknowledged this path more than once")
		}
		handled[path] = struct{}{}
	}
	for _, path := range expectedPaths {
		if _, exists := handled[path]; !exists {
			return NativeJoinHintFragments{}, nativeJoinHintPlanError(path, "native JOIN hint translator did not acknowledge this preference")
		}
	}
	for path, operator := range fragments.JoinOperators {
		if _, exists := expected[path]; !exists {
			return NativeJoinHintFragments{}, nativeJoinHintPlanError(path, "native JOIN hint translator returned an operator for an unknown path")
		}
		if operator == "" {
			return NativeJoinHintFragments{}, nativeJoinHintPlanError(path, "native JOIN hint translator returned an empty operator")
		}
	}
	return fragments, nil
}

func nativeJoinHintPlanError(path, message string) error {
	return &dal.JoinValidationError{Category: "join_plan", Path: path, Message: message}
}
