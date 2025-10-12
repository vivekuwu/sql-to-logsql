package logsql

import "testing"

func intPtr(v int) *int { return &v }

func TestParseJSONPathSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		wantStrict bool
		wantSteps  []struct {
			key   string
			index *int
		}
	}{
		{
			name:       "dot notation",
			raw:        "$.user.name",
			wantStrict: false,
			wantSteps: []struct {
				key   string
				index *int
			}{
				{key: "user"},
				{key: "name"},
			},
		},
		{
			name:       "strict with bracket key",
			raw:        "strict $.payload['ip']",
			wantStrict: true,
			wantSteps: []struct {
				key   string
				index *int
			}{
				{key: "payload"},
				{key: "ip"},
			},
		},
		{
			name:       "lax with quoted key containing spaces",
			raw:        "lax $.payload[\"user name\"]",
			wantStrict: false,
			wantSteps: []struct {
				key   string
				index *int
			}{
				{key: "payload"},
				{key: "user name"},
			},
		},
		{
			name:       "index access",
			raw:        "$.items[10]",
			wantStrict: false,
			wantSteps: []struct {
				key   string
				index *int
			}{
				{key: "items"},
				{index: intPtr(10)},
			},
		},
		{
			name:       "quoted key with escape",
			raw:        "$.meta[\"first\\\"name\"]",
			wantStrict: false,
			wantSteps: []struct {
				key   string
				index *int
			}{
				{key: "meta"},
				{key: "first\"name"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseJSONPath(tt.raw)
			if err != nil {
				t.Fatalf("parseJSONPath(%q) returned error: %v", tt.raw, err)
			}
			if got.Strict != tt.wantStrict {
				t.Fatalf("expected strict=%v, got %v", tt.wantStrict, got.Strict)
			}
			if len(got.Steps) != len(tt.wantSteps) {
				t.Fatalf("expected %d steps, got %d", len(tt.wantSteps), len(got.Steps))
			}
			for i, step := range got.Steps {
				want := tt.wantSteps[i]
				if step.Key != want.key {
					t.Fatalf("step %d: expected key %q, got %q", i, want.key, step.Key)
				}
				switch {
				case step.Index == nil && want.index == nil:
				case step.Index != nil && want.index != nil && *step.Index == *want.index:
				default:
					t.Fatalf("step %d: expected index %v, got %v", i, want.index, step.Index)
				}
			}
		})
	}
}

func TestParseJSONPathErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		message string
	}{
		{
			name:    "empty",
			raw:     "   ",
			message: "translator: JSON path cannot be empty",
		},
		{
			name:    "missing root",
			raw:     "user.name",
			message: "translator: JSON path must start with $",
		},
		{
			name:    "missing nested field",
			raw:     "$",
			message: "translator: JSON path must reference nested field",
		},
		{
			name:    "unexpected end after dot",
			raw:     "$.",
			message: "translator: JSON path ends unexpectedly after .",
		},
		{
			name:    "empty segment",
			raw:     "$.foo..bar",
			message: "translator: JSON path contains empty segment",
		},
		{
			name:    "unsupported token",
			raw:     "$foo",
			message: `translator: unsupported JSON path token "f"`,
		},
		{
			name:    "unterminated brackets",
			raw:     "$.foo[",
			message: "translator: JSON path has unterminated []",
		},
		{
			name:    "empty bracket segment",
			raw:     "$.foo['   ']",
			message: "translator: JSON path contains empty segment",
		},
		{
			name:    "unsupported bracket token",
			raw:     "$.foo[abc]",
			message: "translator: JSON path contains unsupported token inside []",
		},
		{
			name:    "invalid escape",
			raw:     "$.foo[\"bar\\",
			message: "translator: JSON path has invalid escape sequence",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseJSONPath(tt.raw)
			if err == nil {
				t.Fatalf("expected error for %q", tt.raw)
			}
			te, ok := err.(*TranslationError)
			if !ok {
				t.Fatalf("expected TranslationError, got %T", err)
			}
			if te.Message != tt.message {
				t.Fatalf("unexpected error message: want %q, got %q", tt.message, te.Message)
			}
		})
	}
}

func TestJSONPathHasOnlyKeys(t *testing.T) {
	t.Parallel()

	t.Run("keys only", func(t *testing.T) {
		path := &jsonPath{
			Steps: []jsonPathStep{
				{Key: "user"},
				{Key: "id"},
			},
		}
		keys, ok := path.HasOnlyKeys()
		if !ok {
			t.Fatalf("expected ok for keys only path")
		}
		if len(keys) != 2 || keys[0] != "user" || keys[1] != "id" {
			t.Fatalf("unexpected keys: %#v", keys)
		}
	})

	t.Run("contains index", func(t *testing.T) {
		path := &jsonPath{
			Steps: []jsonPathStep{
				{Key: "items"},
				{Index: intPtr(0)},
			},
		}
		if keys, ok := path.HasOnlyKeys(); ok || keys != nil {
			t.Fatalf("expected keys=nil ok=false, got %#v, %v", keys, ok)
		}
	})

	t.Run("nil path", func(t *testing.T) {
		if keys, ok := (*jsonPath)(nil).HasOnlyKeys(); ok || keys != nil {
			t.Fatalf("expected keys=nil ok=false for nil path, got %#v, %v", keys, ok)
		}
	})
}
