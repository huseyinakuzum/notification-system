package api

import (
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		body    string
		vars    map[string]string
		want    string
		wantErr string
	}{
		{
			name: "substitution happy path",
			body: "Hi {{name}}, your code is {{code}}",
			vars: map[string]string{"name": "Sam", "code": "123"},
			want: "Hi Sam, your code is 123",
		},
		{
			name: "whitespace inside braces handled",
			body: "Hi {{ name }}",
			vars: map[string]string{"name": "Sam"},
			want: "Hi Sam",
		},
		{
			name: "no placeholders unchanged",
			body: "plain text body",
			vars: map[string]string{"name": "Sam"},
			want: "plain text body",
		},
		{
			name:    "missing var names the var",
			body:    "Hi {{name}}, code {{code}}",
			vars:    map[string]string{"name": "Sam"},
			wantErr: "code",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Render(tt.body, tt.vars)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Render() error = nil, want error naming %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Render() error = %q, want it to name %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Render() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Render() = %q, want %q", got, tt.want)
			}
		})
	}
}
