package dalgo2sql

import (
	"regexp"
	"testing"
)

var reVersion = regexp.MustCompile(`\d.\d.\d+(-.+)?`)

func TestVersion(t *testing.T) {
	if !reVersion.MatchString(Version) {
		t.Fatalf("Version is not matching expected pattern %v: %v", reVersion.String(), Version)
	}
}
