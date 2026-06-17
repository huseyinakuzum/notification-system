// Package api implements the REST HTTP surface of the notification system.
package api

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var placeholderRE = regexp.MustCompile(`{{\s*(\w+)\s*}}`)

// Render substitutes {{var}} placeholders in body using vars. It returns an
// error naming every referenced placeholder that has no corresponding key in
// vars; the substitution is only applied when all placeholders resolve.
func Render(body string, vars map[string]string) (string, error) {
	var missing []string
	seen := make(map[string]struct{})
	out := placeholderRE.ReplaceAllStringFunc(body, func(match string) string {
		name := placeholderRE.FindStringSubmatch(match)[1]
		val, ok := vars[name]
		if !ok {
			if _, dup := seen[name]; !dup {
				seen[name] = struct{}{}
				missing = append(missing, name)
			}
			return match
		}
		return val
	})
	if len(missing) > 0 {
		sort.Strings(missing)
		return "", fmt.Errorf("missing template vars: %s", strings.Join(missing, ", "))
	}
	return out, nil
}
